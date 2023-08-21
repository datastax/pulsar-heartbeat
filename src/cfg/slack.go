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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
)

// SlackMessage is the message struct to be posted for Slack
type SlackMessage struct {
	Channel   string `json:"channel"`
	Text      string `json:"text"`
	Username  string `json:"username"`
	IconEmogi string `json:"icon_emogi"`
}

// AlertVerbosity contains attributes required to calculate whether verbose alert is required or not
type AlertVerbosity struct {
	lastAlertTime time.Time
	silenceWindow time.Duration
}

const (
	// LogOnly only logs with no alert notification
	LogOnly = -1 * time.Second
)

// MustAlert returns whether the silence window has expired since the last alert.
func (av *AlertVerbosity) MustAlert() bool {
	return time.Since(av.lastAlertTime) > av.silenceWindow
}

var componentsAlert = util.NewSycMap()

// VerboseAlert is able to reduce the verbosity to Slack channel
func VerboseAlert(component, message string, silenceWindow time.Duration) {
	if silenceWindow < 0 {
		log.Errorf("Alert %s", message)
		return
	}
	if GetConfig().SlackConfig.Verbose {
		Alert(message)
		return
	}
	lastAlertV := componentsAlert.Replace(component, AlertVerbosity{
		lastAlertTime: time.Now(),
		silenceWindow: silenceWindow,
	})

	if alert, ok := lastAlertV.(AlertVerbosity); ok {
		if !alert.MustAlert() {
			log.Errorf("Alert %s", message)
			return
		}
	}
	Alert(message)
}

// Alert alerts to slack, email, text.
func Alert(msg string) {
	log.Errorf("Alert %s", msg)
	if GetConfig().SlackConfig.AlertURL == "" {
		return
	}
	err := SendSlackNotification(GetConfig().SlackConfig.AlertURL, SlackMessage{
		Text: msg,
	})
	if err != nil {
		log.Errorf("slack error %v", err)
	}
}

// SendSlackNotification will post to an 'Incoming Webook' url setup in Slack Apps. It accepts
// some text and the slack channel is saved within Slack.
func SendSlackNotification(webhookURL string, msg SlackMessage) error {
	slackBody, _ := json.Marshal(msg)
	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(slackBody))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	if buf.String() != "ok" {
		return fmt.Errorf("non-ok response returned from Slack, message %s", buf.String())
	}
	return nil
}
