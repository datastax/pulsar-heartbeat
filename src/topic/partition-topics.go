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

package topic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
)

// partition topics can be used to test availabilities of all PartitionTopic

// PartitionTopics data struct is the persistent partition topic name and number of partitions it has
type PartitionTopics struct {
	NumberOfPartitions int
	PulsarURL          string
	TokenSupplier      func() (string, error)
	TrustStore         string
	Tenant             string
	Namespace          string
	PartitionTopicName string
	TopicFullname      string
	BaseAdminURL       string
	log                *log.Entry
}

// NewPartitionTopic creates a PartitionTopic test object
func NewPartitionTopic(url string, tokenSupplier func() (string, error), trustStore, topicFn, adminURL string, numOfPartitions int) (*PartitionTopics, error) {
	isPersistent, tenant, ns, topic, err := util.TokenizeTopicFullName(topicFn)
	if err != nil {
		return nil, err
	}
	if !isPersistent {
		return nil, fmt.Errorf("does not support non-persistent topic in partition topic test")
	}
	return &PartitionTopics{
		NumberOfPartitions: numOfPartitions,
		PulsarURL:          url,
		TokenSupplier:      tokenSupplier,
		TrustStore:         trustStore,
		Tenant:             tenant,
		Namespace:          ns,
		PartitionTopicName: topic,
		TopicFullname:      topicFn,
		BaseAdminURL:       adminURL,
		log:                log.WithFields(log.Fields{"app": "partition topic test"}),
	}, nil
}

// GetPartitionTopic gets the partition topic
func (pt *PartitionTopics) GetPartitionTopic() (bool, error) {
	url := pt.BaseAdminURL + "/admin/v2/persistent/" + pt.Tenant + "/" + pt.Namespace + "/partitioned"

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, nil
	}

	if pt.TokenSupplier != nil {
		token, err := pt.TokenSupplier()
		if err != nil {
			return false, nil
		}
		request.Header.Add("Authorization", "Bearer "+token)
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	response, err := client.Do(request)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		pt.log.Errorf("GET PartitionTopic %s error %v", url, err)
		return false, err
	}

	if response.StatusCode != http.StatusOK {
		pt.log.Errorf("GET PartitionTopic %s response status code %d", url, response.StatusCode)
		return false, err
	}

	var partitionTopic []string
	if err = json.NewDecoder(response.Body).Decode(&partitionTopic); err != nil {
		pt.log.Errorf("GET PartitionTopic %s unmarshal response body error %v", url, err)
		return false, err
	}
	expectedTopic := "persistent://" + pt.Tenant + "/" + pt.Namespace + "/" + pt.PartitionTopicName
	found := false
	pt.log.Debugf("all partition topics %v, expected topic %s", partitionTopic, expectedTopic)
	for _, v := range partitionTopic {
		if expectedTopic == v {
			found = true
		}
	}
	return found, nil
}

// CreatePartitionTopic creates a partition topic
func (pt *PartitionTopics) CreatePartitionTopic() error {
	url := pt.BaseAdminURL + "/admin/v2/persistent/" + pt.Tenant + "/" + pt.Namespace + "/" + pt.PartitionTopicName + "/partitions"
	pt.log.Infof("create partition topic URL %s", url)

	payload := strings.NewReader(strconv.Itoa(pt.NumberOfPartitions))
	request, err := http.NewRequest(http.MethodPut, url, payload)
	if err != nil {
		return nil
	}

	request.Header.Add("Content-Type", "text/plain")
	if pt.TokenSupplier != nil {
		token, err := pt.TokenSupplier()
		if err != nil {
			return nil
		}
		request.Header.Add("Authorization", "Bearer "+token)
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	response, err := client.Do(request)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		pt.log.Errorf("CREATE PartitionTopic %s error %v", url, err)
		return err
	}

	if response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusConflict {
		pt.log.Errorf("CREATE PartitionTopic %s response status code %d", url, response.StatusCode)
		return err
	}

	pt.log.Infof("partition topic %s created %d, statusCode %d", pt.PartitionTopicName, pt.NumberOfPartitions, response.StatusCode)
	return nil
}

// VerifyPartitionTopic verifies existence of the partition topic
// it creates one if it's missing
func (pt *PartitionTopics) VerifyPartitionTopic() error {
	created, err := pt.GetPartitionTopic()
	if err != nil {
		return err
	}
	if created {
		pt.log.Infof("partitioned topic %s already exists", pt.TopicFullname)
		return nil
	}

	return pt.CreatePartitionTopic()
}

// TestPartitionTopic sends multiple messages and to be verified by multiple consumers
func (pt *PartitionTopics) TestPartitionTopic(client pulsar.Client) (time.Duration, error) {

	// notify the main thread with the latency to complete the exit of all consumers
	completeChan := make(chan *util.ConsumerResult, pt.NumberOfPartitions)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait() // only close channel after the consumers processed messages or timedout
		close(completeChan)
	}()

	partitionTopicSuffix := "-partition-"
	// prepare the message
	message := fmt.Sprintf("partition topic test message %v", time.Now())
	receiveTimeout := 60 * time.Second

	pt.log.Infof("create a topic producer %s", pt.TopicFullname)
	// create a pulsar producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           pt.TopicFullname,
		DisableBatching: true,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		producer.Close()
		log.Infof("close producer name %s against topic name %s", producer.Name(), producer.Topic())
	}()

	// start multiple consumers and listens to individual partition topics
	for i := 0; i < pt.NumberOfPartitions; i++ {
		topicName := pt.TopicFullname + partitionTopicSuffix + strconv.Itoa(i)
		pt.log.Infof("subscribe to partition topic %s wait on message %s", topicName, message)
		go util.VerifyMessageByPulsarConsumer(client, topicName, message, receiveTimeout, &wg, completeChan)
	}

	// producer sends multiple messages
	start := time.Now()
	for i := 0; i < pt.NumberOfPartitions; i++ {
		ctx := context.Background()

		// Create a different message to send asynchronously
		msg := pulsar.ProducerMessage{
			Payload: []byte(message),
			Key:     "partitionkey" + strconv.Itoa(i),
		}

		// Attempt to send message asynchronously and handle the response
		producer.SendAsync(ctx, &msg, func(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
			if err != nil {
				log.Errorf("failed to send message over partition topic , error: %v", err)
				errMsg := fmt.Sprintf("fail to instantiate Pulsar client: %v", err)
				// report error and exit
				completeChan <- &util.ConsumerResult{
					Err: errors.New(errMsg),
				}
			} else {
				log.Infof("successfully published message on topic %s ", pt.TopicFullname)
			}
		})
	}

	receivedCounter := 0
	successfulCounter := 0
	ticker := time.NewTicker(receiveTimeout)
	defer ticker.Stop()
	for receivedCounter < pt.NumberOfPartitions {
		select {
		case signal := <-completeChan:
			receivedCounter++
			log.Infof(" received counter %d", receivedCounter)
			if signal.Err != nil {
				log.Errorf("topic %s receive error: %v", pt.TopicFullname, signal.Err)
			} else if signal.InOrderDelivery {
				successfulCounter++
				log.Infof("successfully received counter %d", successfulCounter)
			} else {
				log.Errorf("topic %s failed to receive expected messages", pt.TopicFullname)
			}
			if successfulCounter >= pt.NumberOfPartitions {
				return time.Since(start), nil
			}
		case <-ticker.C:
			return 0, fmt.Errorf("received %d msg with %d successful delivery but timed out to receive all %d messages",
				receivedCounter, successfulCounter, pt.NumberOfPartitions)
		}
	}
	return 0, fmt.Errorf("received %d out of %d messages", successfulCounter, pt.NumberOfPartitions)
}
