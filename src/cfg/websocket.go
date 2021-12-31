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
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"github.com/gorilla/websocket"
)

// PulsarMessage is the required message format for Pulsar Websocket message
type PulsarMessage struct {
	Payload    string                 `json:"payload"`
	Properties map[string]interface{} `json:"properties"`
	Context    string                 `json:"context,omitempty"`
}

// ReceivingMessage is the Pulsar message for socket consumer
type ReceivingMessage struct {
	Payload    string                 `json:"payload"`
	MessageID  string                 `json:"messageId"`
	Properties map[string]interface{} `json:"properties"`
	Context    string                 `json:"context,omitempty"`
}

// AckMessage is the message struct to acknowledge a message
type AckMessage struct {
	MessageID string `json:"messageId"`
}

func (w *WsConfig) reconcileConfig() error {
	wsv2 := "/ws/v2/"
	if !strings.HasPrefix(w.ProducerURL, "ws") {
		w.ProducerURL = w.Scheme + w.Cluster + ":" + w.Port + wsv2 + "producer/" + w.TopicName
	}

	if !strings.HasPrefix(w.ConsumerURL, "ws") {
		w.ConsumerURL = w.Scheme + w.Cluster + ":" + w.Port + wsv2 + "consumer/" + w.TopicName + "/"
		if w.Subscription != "" {
			w.ConsumerURL = w.ConsumerURL + w.Subscription
		} else {
			w.ConsumerURL = w.ConsumerURL + "ws-latency-subscription"
		}
	}

	if w.URLQueryParams != "" {
		w.ConsumerURL = w.ConsumerURL + "?" + w.URLQueryParams
		w.ProducerURL = w.ProducerURL + "?" + w.URLQueryParams
	}
	return nil
}

func tokenAsURLQueryParam(url, token string) string {
	if strings.HasSuffix(url, "?token=") {
		return url + token
	}
	return url
}

// WsLatencyTest latency test for websocket
func WsLatencyTest(producerURL, subscriptionURL string, tokenSupplier func()(string,error)) (MsgResult, error) {
	wsHeaders := http.Header{}
	token := ""
	var err error
	if tokenSupplier != nil {
		token, err = tokenSupplier()
		if err != nil {
			return MsgResult{Latency: failedLatency}, err
		}
		bearerToken := "Bearer " + token
		wsHeaders.Add("Authorization", bearerToken)
	}
	prodURL := tokenAsURLQueryParam(producerURL, token)
	subsURL := tokenAsURLQueryParam(subscriptionURL, token)

	// log.Infof("wss producer connection url %s\n\t\tconsumer url %s\n", prodURL, subsURL)
	prodConn, _, err := websocket.DefaultDialer.Dial(prodURL, wsHeaders)
	if err != nil {
		return MsgResult{Latency: failedLatency}, err
	}
	defer prodConn.Close()

	consConn, _, err := websocket.DefaultDialer.Dial(subsURL, wsHeaders)
	if err != nil {
		return MsgResult{Latency: failedLatency}, err
	}
	defer consConn.Close()

	errChan := make(chan error)
	// do not close errChan since there could be timing issue for Consumer listener to send after the close()
	// GC will do the clean up

	// notify the main thread with the latency to complete the exit
	completeChan := make(chan time.Time, 1)
	defer close(completeChan)

	messageText := fmt.Sprintf("test websocket lantecy %s", time.Now())

	// Consumer listener
	go func(expectedMsg string) {
		defer func() {
			if r := recover(); r != nil {
				log.Infof("Recovered from websocket consumer listener panic %v", r)
			}
		}()

		for wait := true; wait; {
			var msg ReceivingMessage
			err := consConn.ReadJSON(&msg)
			if err != nil {
				log.Infof("ws consumer read error: %v", err)
				errChan <- err
				return
			}
			decoded, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil {
				log.Infof("ws consumer decode error: %v", err)
				errChan <- err
				return
			}
			decodedStr := string(decoded)
			actMsg := &AckMessage{MessageID: msg.MessageID}
			if err = consConn.WriteJSON(actMsg); err != nil {
				log.Infof("ws consumer failed to ack message %s", err.Error())
				errChan <- err
				return
			}

			if decodedStr == expectedMsg {
				wait = false
				completeChan <- time.Now()
			}
		}
	}(messageText)

	// Producer listener
	go func() {
		_, rawBytes, err := prodConn.ReadMessage()
		if err != nil {
			log.Errorf("websocket producer received benign error: %v", err)
			return
		}
		byteMessage, err := base64.StdEncoding.DecodeString(string(rawBytes))
		if err != nil {
			log.Errorf("decode producer response error: %v", err)
			return
		}
		log.Infof("websocket producer received response: %s", string(byteMessage))
	}()

	encodedText := base64.StdEncoding.EncodeToString([]byte(messageText))
	message := &PulsarMessage{Payload: encodedText}

	// for mesaure latency
	sentTime := time.Now()

	err = prodConn.WriteJSON(message)
	if err != nil {
		return MsgResult{Latency: failedLatency}, err
	}

	for {
		select {
		case receivedTime := <-completeChan:
			return MsgResult{Latency: receivedTime.Sub(sentTime)}, nil
		case err := <-errChan:
			log.Errorf("websocket error: %v", err)
			return MsgResult{Latency: failedLatency}, err
		case <-time.After(30 * time.Second):
			return MsgResult{Latency: failedLatency}, fmt.Errorf("timed out without receiving the expect message")
		}
	}
}

// TestWsLatency test all clusters' websocket pub sub latency
func TestWsLatency(config WsConfig) {
	tokenSupplier := util.TokenSupplierWithOverride(config.Token, GetConfig().TokenSupplier())
	expectedLatency := util.TimeDuration(config.LatencyBudgetMs, 2*latencyBudget, time.Millisecond)

	stdVerdict := util.GetStdBucket(config.Cluster)

	result, err := WsLatencyTest(config.ProducerURL, config.ConsumerURL, tokenSupplier)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s websocket latency test Pulsar error: %v", config.Cluster, config.Name, err)
		log.Errorf(errMsg)
	} else if result.Latency > expectedLatency {
		stdVerdict.Add(float64(result.Latency.Milliseconds()))
		errMsg := fmt.Sprintf("cluster %s, %s websocket test message latency %v over the budget %v",
			config.Cluster, config.Name, result.Latency, expectedLatency)
		log.Errorf(errMsg)
		ReportIncident(config.Name, config.Cluster, "websocket persisted latency test failure", errMsg, &config.AlertPolicy)
	} else if stddev, mean, within3Sigma := stdVerdict.Push(float64(result.Latency.Milliseconds())); !within3Sigma {
		errMsg := fmt.Sprintf("cluster %s, websocket test message latency %v over three standard deviation %v ms and mean is %v ms",
			config.Cluster, result.Latency, stddev, mean)
		log.Errorf(errMsg)
		ReportIncident(config.Name, config.Cluster, "websocket persisted latency test failure", errMsg, &config.AlertPolicy)
	} else {
		log.Infof("websocket pubsub succeeded with latency %v expected latency %v on topic %s, cluster %s\n",
			result.Latency, expectedLatency, config.TopicName, config.Cluster)
		ClearIncident(config.Name)
	}

	PromLatencySum(GetGaugeType(websocketSubsystem), config.Cluster, result.Latency)
}

// WebSocketTopicLatencyTestThread tests a message websocket delivery in topic and measure the latency.
func WebSocketTopicLatencyTestThread() {
	configs := GetConfig().WebSocketConfig

	for _, cfg := range configs {
		cfg.reconcileConfig()
		go func(t WsConfig) {
			ticker := time.NewTicker(util.TimeDuration(t.IntervalSeconds, 60, time.Second))
			defer ticker.Stop()
			TestWsLatency(t)
			for {
				select {
				case <-ticker.C:
					TestWsLatency(t)
				}
			}
		}(cfg)
	}
}
