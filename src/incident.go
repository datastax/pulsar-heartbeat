package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
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

	// TODO: these two map are not thread safe.
	// track the downtime, since downtime won't be calculated when producing message works
	// it has different defintion than incident
	// this is only applicable when Pulsar Monitor is deployed within a Pulsar cluster
	downtimeTracker = make(map[string]incidentRecord)

	incidentTrackers     = make(map[string]*IncidentAlertPolicy)
	incidentTrackersLock = &sync.RWMutex{}
)

const (
	opsGenieAlertURL = "https://api.opsgenie.com/v2/alerts"
)

// Incident is the struct for incident reporting
type Incident struct {
	Message     string   `json:"message"`
	Description string   `json:"description"`
	Priority    string   `json:"priority"`
	Entity      string   `json:"entity"`
	Alias       string   `json:"alias"`
	Tags        []string `json:"tags"`
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
}

// return if alert is triggered
func (t *IncidentAlertPolicy) report(component, msg, desc string) bool {
	t.Entity = component
	t.Counters = t.Counters + 1
	t.Alerts[time.Now()] = true
	if t.Limit > 0 && t.Counters >= t.Limit {
		// TODO: to be discussed if resetting to 0 is too relaxed
		t.Counters = 0
		t.Alerts = make(map[time.Time]bool)
		return true
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

// ReportIncident reports an incident.
func ReportIncident(component, alias, msg, desc string, eval *AlertPolicyCfg) {
	if eval.Ceiling > 0 && trackIncident(component, msg, desc, eval) {
		CreateIncident(component, alias, msg, desc, "P2")
		AnalyticsReportIncident(component, alias, msg, desc)
	}
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
	}
}

// CreateIncident creates incident
func CreateIncident(component, alias, msg, desc, priority string) {
	genieKey := GetConfig().OpsGenieConfig.AlertKey
	err := CreateOpsGenieAlert(NewIncident(component, alias, msg, desc, priority), genieKey)
	if err != nil {
		Alert(fmt.Sprintf("Opsgenie report incident error %v", err))
	}
}

// RemoveIncident removes an existing incident
func RemoveIncident(component string) {
	incidentsLock.RLock()
	record, ok := incidents[component]
	incidentsLock.RUnlock()

	if ok {
		incidentsLock.Lock()
		delete(incidents, component)
		incidentsLock.Unlock()

		downtimeDuration := time.Since(record.createdAt)
		seconds := int(downtimeDuration.Seconds())
		AnalyticsClearIncident(component, seconds)
		PromLatencySum(PubSubDowntimeGaugeOpt(), component, downtimeDuration)

		if record.alertID == "" {
			log.Errorf("%s unable to identify alert with request id %s for auto clear operation", component, record.requestID)
			return
		}
		log.Infof("auto record alertID %v", record)
		genieKey := GetConfig().OpsGenieConfig.AlertKey
		err := CloseOpsGenieAlert(component, record.alertID, genieKey)
		if err != nil {
			Alert(fmt.Sprintf("Opsgenie remove incident error %v", err))
		}
	}
}

// CalculateDowntime calculate downtime
func CalculateDowntime(component string) {
	if record, ok := downtimeTracker[component]; ok {
		delete(downtimeTracker, component)
		seconds := int(time.Since(record.createdAt).Seconds())
		AnalyticsDowntime(component, seconds)
	}
}

func opsGenieHTTP(method, endpoint, genieKey string, payload *bytes.Buffer) (*http.Response, error) {
	if genieKey == "" {
		errStr := fmt.Sprintf("Alert creation failed. %s has not configured with genieKey.", Config.Name)
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
	Alert(fmt.Sprintf("report incident as pager escalation %v", msg))

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
	downtimeTracker[msg.Entity] = incident

	// there is a delay when the alert is created by opsgenie, so we use retry
	// time out has to be less than the latency time interval
	go getOpsGenieAlertIDRetry(msg.Entity, incident.requestID, genieKey, 4*time.Second)
	return nil
}

func getOpsGenieAlertIDRetry(entity, requestID, genieKey string, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		time.Sleep(200 * time.Millisecond) //TODO: could have exponatial back off retry
		alertID, err := getOpsGenieAlertID(requestID, genieKey)
		if err == nil {
			incidentsLock.Lock()
			incident, ok := incidents[entity]
			if ok {
				incident.alertID = alertID
				incidents[entity] = incident
			}
			log.Infof("found...")
			incidentsLock.Unlock()
			return
		}
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
