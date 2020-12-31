//
//  Copyright (c) 2020-2021 Datastax, Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
//

package cfg

import (
	"fmt"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-monitor/src/k8s"
	"github.com/datastax/pulsar-monitor/src/util"
)

const clusterMonInterval = 10 * time.Second

// ClusterHealth a cluster health struct
type ClusterHealth struct {
	sync.RWMutex
	Status         k8s.ClusterStatusCode
	MissingBrokers int
}

var clusterHealth = ClusterHealth{}

// Get gets the cluster health status
func (h *ClusterHealth) Get() (k8s.ClusterStatusCode, int) {
	h.RLock()
	defer h.RUnlock()
	return h.Status, h.MissingBrokers
}

// Set sets the cluster health status
func (h *ClusterHealth) Set(status k8s.ClusterStatusCode, offlineBrokers int) {
	h.Lock()
	h.Status = status
	h.MissingBrokers = offlineBrokers
	h.Unlock()
}

// EvaluateClusterHealth evaluates and reports the k8s cluster health
func EvaluateClusterHealth(client *k8s.Client) error {
	k8sCfg := GetConfig().K8sConfig
	cluster := GetConfig().Name + "-in-cluster"
	ns := util.AssignString(k8sCfg.PulsarNamespace, k8s.DefaultPulsarNamespace)
	// again this is for in-cluster monitoring only

	if err := client.UpdateReplicas(ns); err != nil {
		return err
	}
	if err := client.WatchPods(ns); err != nil {
		return err
	}
	desc, status := client.EvalHealth()
	clusterHealth.Set(status.Status, status.BrokerOfflineInstances)

	PromGaugeInt(GetOfflinePodsCounter(k8sZookeeperSubsystem), cluster, status.ZookeeperOfflineInstances)
	PromGaugeInt(GetOfflinePodsCounter(k8sBookkeeperSubsystem), cluster, status.BookkeeperOfflineInstances)
	PromGaugeInt(GetOfflinePodsCounter(k8sBrokerSubsystem), cluster, status.BrokerOfflineInstances)
	PromGaugeInt(GetOfflinePodsCounter(k8sProxySubsystem), cluster, status.ProxyOfflineInstances)

	if status.Status != k8s.OK {
		errMsg := fmt.Sprintf("cluster %s, k8s pulsar cluster status is unhealthy, error message %s", cluster, desc)
		if status.Status == k8s.TotalDown {
			VerboseAlert(cluster, errMsg, 3*time.Minute)
			ReportIncident(cluster, cluster, "kubernete cluster is down, reported by pulsar-monitor", errMsg, &k8sCfg.AlertPolicy)
		}
	} else {
		ClearIncident(cluster)
	}
	log.Infof("k8s cluster status %v", status)
	return nil
}

// MonitorK8sPulsarCluster start K8sPulsarClusterMonitor thread
func MonitorK8sPulsarCluster() error {
	k8sCfg := GetConfig().K8sConfig
	if !k8sCfg.Enabled {
		return nil
	}

	ns := util.AssignString(k8sCfg.PulsarNamespace, k8s.DefaultPulsarNamespace)
	clientset, err := k8s.GetK8sClient(ns)
	if err != nil {
		log.Errorf("failed to get k8s clientset %v or get pods under pulsar namespace", err)
		return err
	}

	go func(client *k8s.Client) {
		log.Infof("start k8s cluster monitoring ...")
		ticker := time.NewTicker(clusterMonInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := EvaluateClusterHealth(clientset); err != nil {
					log.Errorf("k8s monitoring failed to watchpods error: %v", err)
				}
			}
		}

	}(clientset)
	return nil
}
