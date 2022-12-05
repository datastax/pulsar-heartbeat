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

// monitor individual broker health
// monitor load balance, and the number of topics balance on broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
)

var statsLog = log.WithFields(log.Fields{"app": "broker health monitor"})

const (
	topicStatsDBTable = "topic-stats"
)

// GetBrokers gets a list of brokers and ports
func GetBrokers(restBaseURL, clusterName string, tokenSupplier func() (string, error)) ([]string, error) {
	brokersURL := util.SingleSlashJoin(restBaseURL, "admin/v2/brokers/"+clusterName)
	newRequest, err := http.NewRequest(http.MethodGet, brokersURL, nil)
	if err != nil {
		return nil, err
	}
	newRequest.Header.Add("user-agent", "pulsar-heartbeat")
	if tokenSupplier != nil {
		token, err := tokenSupplier()
		if err != nil {
			return nil, err
		}
		newRequest.Header.Add("Authorization", "Bearer "+token)
	}
	client := &http.Client{
		CheckRedirect: util.PreserveHeaderForRedirect,
		Timeout:       10 * time.Second,
	}
	resp, err := client.Do(newRequest)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode > 300 {
		return nil, fmt.Errorf("failed to get a list of brokers, returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	brokers := []string{}
	err = json.Unmarshal(bodyBytes, &brokers)
	if err != nil {
		return nil, err
	}

	return brokers, nil
}

// required configuration cluster name and broker url
// 1. get a list of broker ips
// 2. query each broker individually
//

// BrokerTopicsQuery returns a map of broker and topic full name, or error of this operation
func BrokerTopicsQuery(brokerBaseURL, token string) ([]string, error) {
	// key is tenant, value is partition topic name
	if !strings.HasPrefix(brokerBaseURL, "http") {
		brokerBaseURL = "http://" + brokerBaseURL
	}
	topicStatsURL := util.SingleSlashJoin(brokerBaseURL, "admin/v2/broker-stats/topics")
	statsLog.Debugf(" proxy request route is %s\n", topicStatsURL)

	newRequest, err := http.NewRequest(http.MethodGet, topicStatsURL, nil)
	if err != nil {
		return nil, err
	}
	newRequest.Header.Add("user-agent", "pulsar-heartbeat")
	newRequest.Header.Add("Authorization", "Bearer "+token)
	client := &http.Client{
		CheckRedirect: util.PreserveHeaderForRedirect,
		Timeout:       10 * time.Second,
	}
	response, err := client.Do(newRequest)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		statsLog.Errorf("GET broker topic stats request %s error %v", topicStatsURL, err)
		return nil, err
	}

	var namespaces map[string]map[string]map[string]map[string]interface{}
	if err = json.Unmarshal(body, &namespaces); err != nil {
		return nil, err
	}

	topics := []string{}
	for k, v := range namespaces {
		tenant := strings.Split(k, "/")[0]
		statsLog.Debugf("namespace %s tenant %s", k, tenant)

		for bundleKey, v2 := range v {
			statsLog.Debugf("  bundle %s", bundleKey)
			for persistentKey, v3 := range v2 {
				statsLog.Debugf("    %s key", persistentKey)

				for topicFn := range v3 {
					// statsLog.Infof("      topic name %s", topicFn)
					topics = append(topics, topicFn)
				}
			}
		}
	}
	return topics, nil
}

// ConnectBrokerHealthcheckTopic reads the latest messages off broker's healthcheck topic
func ConnectBrokerHealthcheckTopic(brokerURL, clusterName, pulsarURL string, tokenSupplier func() (string, error), completeChan chan error) {
	// "persistent://pulsar/{cluster}/10.244.7.85:8080/healthcheck"
	brokerAddr := util.SingleSlashJoin(strings.ReplaceAll(brokerURL, "http://", ""), "healthcheck")
	defer func() {
		// the channel has been closed by the main EvaluateBrokers
		if recover() != nil {
			log.Errorf("cluster %s individual broker %s test timed out", clusterName, brokerAddr)
		}
	}()
	client, err := GetPulsarClient(pulsarURL, tokenSupplier)
	if err != nil {
		completeChan <- err
		return
	}

	topicName := "persistent://pulsar/" + clusterName + "/" + brokerAddr
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          topicName,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		completeChan <- err
		return
	}
	defer reader.Close()

	ctx := context.Background()

	statsLog.Debugf("created reader on topic %s", topicName)
	//
	found := false
	for reader.HasNext() && !found {
		msg, err := reader.Next(ctx)
		if err != nil {
			completeChan <- err
			return
		}
		found = time.Now().Sub(msg.PublishTime()) < 120*time.Second
		statsLog.Debugf("Received message : publish time %v %v", msg.PublishTime(), found)
	}

	if found {
		completeChan <- nil
		return
	}
	completeChan <- fmt.Errorf("failed to get message on topic %s", topicName)
}

// EvaluateBrokers evaluates all brokers' health
func EvaluateBrokers(urlPrefix, clusterName, pulsarURL string, tokenSupplier func() (string, error), duration time.Duration) (int, error) {
	brokers, err := GetBrokers(urlPrefix, clusterName, tokenSupplier)
	if err != nil {
		return 0, err
	}

	statsLog.Infof("a list of brokers %v", brokers)
	failedBrokers := 0
	errStr := ""
	// notify the main thread with the latency to complete the exit of all consumers
	completeChan := make(chan error, len(brokers))
	defer close(completeChan)

	for _, brokerURL := range brokers {
		go ConnectBrokerHealthcheckTopic(brokerURL, clusterName, pulsarURL, tokenSupplier, completeChan)
	}

	receivedCounter := 0
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	for receivedCounter < len(brokers) {
		select {
		case signal := <-completeChan:
			receivedCounter++
			statsLog.Infof(" broker received counter %d", receivedCounter)
			if signal != nil {
				failedBrokers++
				errStr = errStr + signal.Error() + ";"
			}
		case <-ticker.C:
			return failedBrokers, fmt.Errorf("received %d msg but timed out to receive all %d messages",
				receivedCounter, len(brokers))
		}
	}

	statsLog.Infof("cluster %s has %d failed brokers out of total %d brokers", clusterName, failedBrokers, len(brokers))
	if errStr != "" {
		return failedBrokers, fmt.Errorf(errStr)
	}

	return failedBrokers, nil
}

// TestBrokers evaluates and reports all brokers health
func TestBrokers(topicCfg TopicCfg) error {
	if topicCfg.ClusterName == "" {
		return nil
	}
	name := topicCfg.ClusterName + "-brokers"

	tokenSupplier := util.TokenSupplierWithOverride(topicCfg.Token, GetConfig().TokenSupplier())

	intervalDuration := 10 * time.Second
	if topicCfg.IntervalSeconds > 20 {
		intervalDuration = time.Duration(topicCfg.IntervalSeconds/2) * time.Second
	}
	failedBrokers, err := EvaluateBrokers(topicCfg.AdminURL, topicCfg.ClusterName, topicCfg.PulsarURL, tokenSupplier, intervalDuration)

	if failedBrokers > 0 {
		errMsg := fmt.Sprintf("cluster %s has %d unhealthy brokers, error message: %v", name, failedBrokers, err)
		log.Errorf(errMsg)
		ReportIncident(name, name, "brokers are unhealthy reported by pulsar-heartbeat", errMsg, &topicCfg.AlertPolicy)
	} else if err != nil {
		errMsg := fmt.Sprintf("cluster %s Pulsar brokers test failed, error message: %v", name, err)
		log.Errorf(errMsg)
		ReportIncident(name, name, "brokers test error reported by pulsar-heartbeat", errMsg, &topicCfg.AlertPolicy)
	} else {
		statsLog.Infof("%s broker test has successfully passed", name)
		ClearIncident(name)
	}
	return nil
}
