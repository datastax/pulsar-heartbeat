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
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-monitor/src/brokers"
	"github.com/datastax/pulsar-monitor/src/util"
)

// EvaluateBrokers evaluates and reports all brokers health
func EvaluateBrokers(prefixURL, token string) error {
	name := GetConfig().Name + "-brokers" // again this is for in-cluster monitoring only

	brokerCfg := GetConfig().BrokersConfig
	clusterName := util.AssignString(GetConfig().ClusterName, GetConfig().Name)
	failedBrokers, err := brokers.TestBrokers(prefixURL, clusterName, token)
	if err != nil || failedBrokers > 0 {
		// retry since all test calls are http based
		time.Sleep(200 * time.Millisecond)
		failedBrokers, err = brokers.TestBrokers(prefixURL, clusterName, token)
	}

	if failedBrokers > 0 {
		errMsg := fmt.Sprintf("cluster %s has %d unhealthy brokers, error message %v", name, failedBrokers, err)
		VerboseAlert(name+"-broker", errMsg, 3*time.Minute)
		ReportIncident(name, name, "brokers are unhealthy reported by pulsar-monitor", errMsg, &brokerCfg.AlertPolicy)
	} else if err != nil {
		errMsg := fmt.Sprintf("cluster %s Pulsar brokers test failed, error message %v", name, err)
		VerboseAlert(name+"-broker", errMsg, 3*time.Minute)
	} else {
		ClearIncident(name)
	}
	return nil
}

// MonitorBrokers start K8sPulsarClusterMonitor thread
func MonitorBrokers() error {
	token := GetConfig().Token
	if token == "" {
		log.Infof("MonitorBroker exits since no token is specified")
		return nil
	}

	prefixURL := GetConfig().BrokersConfig.InClusterRESTURL
	if prefixURL == "" {
		log.Infof("MonitorBroker exits since no in-cluster REST URL prefix is specified")
		return nil
	}

	interval := util.TimeDuration(GetConfig().BrokersConfig.IntervalSeconds, 60, time.Second)

	go func(restURL, jwt string, loopInterval time.Duration) {
		log.Infof("start all brokers monitoring every %v...", loopInterval)
		ticker := time.NewTicker(loopInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := EvaluateBrokers(restURL, jwt); err != nil {
					log.Errorf("pulsar brokers monitoring failed, error: %v", err)
				}
			}
		}
	}(prefixURL, token, interval)
	return nil
}
