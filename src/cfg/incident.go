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
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/datastax/pulsar-heartbeat/src/util"
	"github.com/hashicorp/go-retryablehttp"
)

// report incident which usually is high level of escalation and paging
// incident tracker and policy are NOT thread safe struct and map.

type incidentRecord struct {
	requestID string
	alertID   string
	createdAt time.Time
}

var (
	// AllowedPriorities a list of allowed priorities
	AllowedPriorities = []string{"P1", "P2", "P3", "P4", "P5"}

	// key is incident identifier, value is OpsGenie requestId for delete purpose
	incidents = make(map[string]incidentRecord)

	// lock for incidents map
	incidentsLock = &sync.RWMutex{}

	// tracks incident to determine whether real alerting is required
	// key is the component name
	incidentTrackers     = make(map[string]*IncidentAlertPolicy)
	incidentTrackersLock = &sync.RWMutex{}
)

const (
	opsGenieAlertURL = "https://api.opsgenie.com/v2/alerts"
)

// Incident is the struct for incident reporting
type Incident struct {
	Message     string    `json:"message"`
	Description string    `json:"description"`
	Priority    string    `json:"priority"`
	Entity      string    `json:"entity"`
	Alias       string    `json:"alias"`
	Tags        []string  `json:"tags"`
	Timestamp   time.Time `json:"timestamp"`
}

// OpsGenieAlertCreateResponse is the response struct returned by OpsGenie
// https://docs.opsgenie.com/docs/alert-api#section-create-alert
type OpsGenieAlertCreateResponse struct {
	Result    string  `json:"result"`
	Took      float64 `json:"took"`
	RequestID string  `json:"requestId"`
}

// OpsGenieAlertGetResponse is the response struct returned by OpsGenie
// https://docs.opsgenie.com/docs/alert-api#section-create-alert
type OpsGenieAlertGetResponse struct {
	Data      alertGetData `json:"data"`
	Took      float64      `json:"took"`
	RequestID string       `json:"requestId"`
}

// alertGetData is data part of OpsGenieAlertGetResponse
// only creates the attributes that we are interested
type alertGetData struct {
	Success   bool   `json:"success"`
	IsSuccess bool   `json:"isSuccess"`
	Status    string `json:"status"`
	AlertID   string `json:"alertId"`
	Alias     string `json:"alias"`
}

// OpsGenieAlertCloseRequest is the POST request payload json
type OpsGenieAlertCloseRequest struct {
	User   string `json:"user"`
	Source string `json:"source"`
	Note   string `json:"note"`
}

// IncidentAlertPolicy tracks and reports incident when threshold is reached
type IncidentAlertPolicy struct {
	Entity            string
	Counters          int
	EvalWindowSeconds time.Duration
	Alerts            map[time.Time]bool
	LimitInWindow     int
	Limit             int
	LastUpdatedAt     time.Time
}

// return if alert is triggered
func (t *IncidentAlertPolicy) report(component, msg, desc string) bool {
	t.LastUpdatedAt = time.Now()
	t.Entity = component
	t.Counters = t.Counters + 1
	t.Alerts[time.Now()] = true
	if t.Limit > 0 && t.Counters >= t.Limit {
		// TODO: to be discussed if resetting to 0 is too relaxed
		t.Counters = 0
		t.Alerts = make(map[time.Time]bool)
		return true
	}
	if t.Limit > 0 && t.Counters+1 >= t.Limit {
		// pre-alert before an incident could be created next time
		VerboseAlert(component, msg, time.Hour)
	}

	windowCounts := 0
	for v := range t.Alerts {
		if time.Now().Sub(v) < t.EvalWindowSeconds {
			windowCounts++
		} else {
			//evict expired alert
			delete(t.Alerts, v)
		}
	}

	if t.LimitInWindow > 0 && windowCounts >= t.LimitInWindow {
		t.Counters = 0
		t.Alerts = make(map[time.Time]bool)
		return true
	}
	return false
}

func (t *IncidentAlertPolicy) clear() int {
	t.Counters--
	return t.Counters
}

func newPolicy(component, msg, desc string, eval *AlertPolicyCfg) IncidentAlertPolicy {
	newTracker := IncidentAlertPolicy{}
	newTracker.EvalWindowSeconds = util.TimeDuration(eval.MovingWindowSeconds, 1, time.Second)
	newTracker.Alerts = make(map[time.Time]bool)
	newTracker.LimitInWindow = eval.CeilingInMovingWindow
	newTracker.Limit = eval.Ceiling
	newTracker.LastUpdatedAt = time.Now()
	return newTracker
}

func trackIncident(component, msg, desc string, eval *AlertPolicyCfg) bool {
	incidentTrackersLock.Lock()
	defer incidentTrackersLock.Unlock()
	if tracker, ok := incidentTrackers[component]; ok {
		return tracker.report(component, msg, desc)
	}
	t := newPolicy(component, msg, desc, eval)
	rc := t.report(component, msg, desc)
	incidentTrackers[component] = &t
	return rc
}

// ReportIncident reports an incident return bool indicate an incident is created or not.
func ReportIncident(component, alias, msg, desc string, eval *AlertPolicyCfg) bool {
	if eval.Ceiling > 0 && trackIncident(component, msg, desc, eval) {
		CreateIncident(component, alias, msg, desc, "P2")
		return true
	}

	count := 0
	incidentTrackersLock.RLock()
	// create an incident when multiple (3) components fails altogether near each other
	// only run when this is a single in-cluster monitoring
	if GetConfig().K8sConfig.Enabled && len(incidentTrackers) > 2 {
		for _, v := range incidentTrackers {
			if time.Since(v.LastUpdatedAt) < time.Minute {
				count++
			}
		}
	}
	incidentTrackersLock.RUnlock()

	if count > 2 {
		CreateIncident(component, alias, msg, desc, "P2")
		return true
	}
	return false
}

// ClearIncident clears an incident
func ClearIncident(component string) {
	RemoveIncident(component)

	incidentTrackersLock.Lock()
	defer incidentTrackersLock.Unlock()
	if tracker, ok := incidentTrackers[component]; ok {
		if tracker.clear() == 0 {
			delete(incidentTrackers, component)
		}
	}
}

// NewIncident creates a Incident object
func NewIncident(component, alias, msg, desc, priority string) Incident {
	p := "P2" //default priority
	if util.StrContains(AllowedPriorities, priority) {
		p = priority
	}
	return Incident{
		Message:     msg,
		Description: desc,
		Priority:    p,
		Entity:      component,
		Alias:       alias,
		Tags:        []string{"ops-monitor", component},
		Timestamp:   time.Now(),
	}
}

// CreateIncident creates incident
func CreateIncident(component, alias, msg, desc, priority string) {
	Alert(fmt.Sprintf("report incident as pager escalation, component %s, alias %s, message %s, description %s",
		component, alias, msg, desc))
	genieKey := GetConfig().OpsGenieConfig.AlertKey
	if genieKey != "" {
		err := CreateOpsGenieAlert(NewIncident(component, alias, msg, desc, priority), genieKey)
		if err != nil {
			Alert(fmt.Sprintf("from %s Opsgenie report incident error %v", component, err))
		}
	}

	if GetConfig().PagerDutyConfig.IntegrationKey != "" {
		err := CreatePDIncident(component, alias, msg, GetConfig().PagerDutyConfig.IntegrationKey)
		if err != nil {
			Alert(fmt.Sprintf("from %s PagerDuty report incident error %v", component, err))
		}
	}
}

// RemoveIncident removes an existing incident
func RemoveIncident(component string) {
	incidentsLock.Lock()
	record, ok := incidents[component]
	delete(incidents, component)
	incidentsLock.Unlock()

	if ok {
		if record.alertID == "" {
			log.Errorf("%s unable to identify alert with request id %s for auto clear operation", component, record.requestID)
			return
		}
		log.Infof("auto record alertID %v", record)
		genieKey := GetConfig().OpsGenieConfig.AlertKey
		if genieKey != "" {
			err := CloseOpsGenieAlert(component, record.alertID, genieKey)
			if err != nil {
				Alert(fmt.Sprintf("from %s Opsgenie remove incident error %v", component, err))
			}
		}

		ResolvePDIncident(component, record.alertID, GetConfig().PagerDutyConfig.IntegrationKey)
	}
}

func opsGenieHTTP(method, endpoint, genieKey string, payload *bytes.Buffer) (*http.Response, error) {
	if genieKey == "" {
		errStr := fmt.Sprintf("Alert creation failed. %s has not configured with genieKey.", GetConfig().Name)
		Alert(errStr)
		return nil, fmt.Errorf(errStr)
	}

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	log.Infof("method %v request URL %v", method, opsGenieAlertURL+endpoint)
	req, err := retryablehttp.NewRequest(method, opsGenieAlertURL+endpoint, payload)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", genieKey)
	req.Header.Set("Content-Type", "application/json")

	return client.Do(req)
}

// CreateOpsGenieAlert creates an OpsGenie alert
func CreateOpsGenieAlert(msg Incident, genieKey string) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	resp, err := opsGenieHTTP(http.MethodPost, "", genieKey, bytes.NewBuffer(buf))
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	if resp.StatusCode > 300 {
		return fmt.Errorf("Create Opsgenie alert returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	alertResp := OpsGenieAlertCreateResponse{}
	err = json.Unmarshal(bodyBytes, &alertResp)
	if err != nil {
		return err
	}

	incident := incidentRecord{
		requestID: alertResp.RequestID,
		createdAt: time.Now(),
	}

	incidentsLock.Lock()
	defer incidentsLock.Unlock()
	incidents[msg.Entity] = incident

	// there is a delay when the alert is created by opsgenie, so we use retry
	// time out has to be less than the latency time interval
	go getOpsGenieAlertIDRetry(msg.Entity, incident.requestID, genieKey, 4*time.Second)
	return nil
}

// verify and get created alert's alertID for auto resolve purpose
func getOpsGenieAlertIDRetry(entity, requestID, genieKey string, timeout time.Duration) {
	start := time.Now()
	waitDuration := 200 * time.Millisecond
	for time.Since(start) < timeout {
		time.Sleep(waitDuration)
		alertID, err := getOpsGenieAlertID(requestID, genieKey)
		if err == nil {
			incidentsLock.Lock()
			incident, ok := incidents[entity]
			if ok {
				incident.alertID = alertID
				incidents[entity] = incident
			}
			incidentsLock.Unlock()
			return
		}
		waitDuration = waitDuration * 2
	}
	log.Errorf("%s unable to find alert with requestId %s", entity, requestID)
}

// getOpsGenieAlertID gets alertID from a created alert.
// alertID is used for alert clear purpose
func getOpsGenieAlertID(requestID, genieKey string) (alertID string, err error) {
	resp, err := opsGenieHTTP(http.MethodGet, "/requests/"+requestID, genieKey, &bytes.Buffer{})
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return "", err
	}

	if resp.StatusCode > 300 {
		return "", fmt.Errorf("Get Opsgenie alert returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	alertResp := OpsGenieAlertGetResponse{}
	err = json.Unmarshal(bodyBytes, &alertResp)
	if err != nil {
		return "", err
	}

	return alertResp.Data.AlertID, nil
}

// CloseOpsGenieAlert deletes an OpsGenie alert
func CloseOpsGenieAlert(component, alertID string, genieKey string) error {
	buf, err := json.Marshal(OpsGenieAlertCloseRequest{
		User:   "pulsar monitor",
		Source: component,
		Note:   "*automatically resolved the alert* (alertId) " + alertID,
	})
	if err != nil {
		return err
	}

	resp, err := opsGenieHTTP(http.MethodPost, fmt.Sprintf("/%s/close?identifierType=id", alertID), genieKey, bytes.NewBuffer(buf))
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	if resp.StatusCode > 300 {
		return fmt.Errorf("Close Opsgenie alert returns incorrect status code %d", resp.StatusCode)
	}
	return nil
}
