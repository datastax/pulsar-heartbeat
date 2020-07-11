package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kafkaesque-io/pulsar-monitor/src/k8s"
)

// K8s pulsar cluster monitor

// ClusterHealth a cluster health struct
type ClusterHealth struct {
	sync.RWMutex
	Status k8s.ClusterStatus
}

// Get gets the cluster health status
func (h *ClusterHealth) Get() k8s.ClusterStatus {
	h.RLock()
	h.RUnlock()
	return h.Status
}

// Set sets the cluster health status
func (h *ClusterHealth) Set(status k8s.ClusterStatus) {
	h.Lock()
	h.Status = status
	h.Unlock()
}

// EvaluateClusterHealth evaluates and reports the k8s cluster health
func EvaluateClusterHealth(client *k8s.Client) error {
	cfg := GetConfig().K8sConfig
	cluster := GetConfig().Name // again this is for in-cluster monitoring only

	if err := client.UpdateReplicas(); err != nil {
		return err
	}
	if err := client.WatchPods(k8s.DefaultPulsarNamespace); err != nil {
		return err
	}
	desc, status := client.EvalHealth()
	clusterHealth.Set(status)
	if status != k8s.OK {
		errMsg := fmt.Sprintf("cluster %s, k8s pulsar cluster status is unhealthy, error message %s", cluster, desc)
		Alert(errMsg)
		if status == k8s.TotalDown {
			ReportIncident(cluster, cluster, "persisted latency test failure", errMsg, &cfg.AlertPolicy)
		}
	}
	log.Printf("k8 cluster status %d", status)
	return nil
}

// MonitorK8sPulsarCluster start K8sPulsarClusterMonitor thread
func MonitorK8sPulsarCluster() error {
	cfg := GetConfig().K8sConfig
	if !cfg.Enabled {
		return nil
	}

	clientset, err := k8s.GetK8sClient()
	if err != nil {
		log.Printf("failed to get k8s clientset %v or get pods under pulsar namespace", err)
		return err
	}

	go func(client *k8s.Client) {
		log.Println("start k8s cluster monitoring ...")
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				if err := EvaluateClusterHealth(clientset); err != nil {
					log.Printf("k8s monitoring failed to watchpods error: %v", err)
				}
			}
		}

	}(clientset)
	return nil
}
