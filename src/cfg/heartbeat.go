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
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/apex/log"
	"github.com/hashicorp/go-retryablehttp"
)

// StartHeartBeat starts heartbeat monitoring the program by OpsGenie
func StartHeartBeat() {
	// opsgenie url in the format of "https://api.opsgenie.com/v2/heartbeats/<component>/ping"
	genieURL := GetConfig().OpsGenieConfig.HeartBeatURL
	genieKey := GetConfig().OpsGenieConfig.HeartbeatKey
	if genieURL != "" && genieKey != "" {
		err := HeartBeatToOpsGenie(genieURL, genieKey)
		if err != nil {
			Alert(fmt.Sprintf("OpsGenie error %v", err))
		}
	}
}

// UptimeHeartBeat sends heartbeat to uptime counter
func UptimeHeartBeat() {
	AnalyticsHeartbeat(GetConfig().Name)
	PromCounter(HeartbeatCounterOpt(), GetConfig().Name)
}

// HeartBeatToOpsGenie send heart beat to ops genie
func HeartBeatToOpsGenie(genieURL, genieKey string) error {

	name := GetConfig().Name

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	req, err := retryablehttp.NewRequest(http.MethodGet, genieURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", genieKey)

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		Alert(fmt.Sprintf("from %s Opsgenie returns error %v", name, err))
		return err
	}

	log.Infof("opsgenie status code %d", resp.StatusCode)
	if resp.StatusCode > 300 {
		msg := fmt.Sprintf("from %s Opsgenie returns incorrect status code %d", name, resp.StatusCode)
		Alert(msg)
		return errors.New(msg)
	}

	return nil
}
