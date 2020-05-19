package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// report incident which usually is high level of escalation and paging
// incident tracker and policy are NOT thread safe struct and map.

type incidentRecord struct {
	requestID string
	createdAt time.Time
}

var (
	// AllowedPriorities a list of allowed priorities
	AllowedPriorities = []string{"P1", "P2", "P3", "P4", "P5"}

	// key is incident identifier, value is OpsGenie requestId for delete purpose
	incidents = make(map[string]incidentRecord)

	incidentTrackers = make(map[string]*IncidentAlertPolicy)
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

// OpsGenieAlertResponse is the response struct returned by OpsGenie
// https://docs.opsgenie.com/docs/alert-api#section-create-alert
type OpsGenieAlertResponse struct {
	Result    string  `json:"result"`
	Took      float64 `json:"took"`
	RequestID string  `json:"requestId"`
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
	newTracker.EvalWindowSeconds = TimeDuration(eval.MovingWindowSeconds, 1, time.Second)
	newTracker.Alerts = make(map[time.Time]bool)
	newTracker.LimitInWindow = eval.CeilingInMovingWindow
	newTracker.Limit = eval.Ceiling
	return newTracker
}

func trackIncident(component, msg, desc string, eval *AlertPolicyCfg) bool {
	if tracker, ok := incidentTrackers[component]; ok {
		return tracker.report(component, msg, desc)
	}
	t := newPolicy(component, msg, desc, eval)
	rc := t.report(component, msg, desc)
	incidentTrackers[component] = &t
	return rc
}

// ReportIncident reports an incident
func ReportIncident(component, alias, msg, desc string, eval *AlertPolicyCfg) {
	if eval.Ceiling > 0 && trackIncident(component, msg, desc, eval) {
		CreateIncident(component, alias, msg, desc, "P2")
		AnalyticsReportIncident(component, alias, msg, desc)
	}
}

// ClearIncident clears an incident
func ClearIncident(component string) {
	RemoveIncident(component)

	if tracker, ok := incidentTrackers[component]; ok {
		if tracker.clear() == 0 {
			delete(incidentTrackers, component)
		}
	}
}

// NewIncident creates a Incident object
func NewIncident(component, alias, msg, desc, priority string) Incident {
	p := "P2" //default priority
	if StrContains(AllowedPriorities, priority) {
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
	if record, ok := incidents[component]; ok {
		delete(incidents, component)
		seconds := int(time.Since(record.createdAt).Seconds())
		AnalyticsClearIncident(component, seconds)

		genieKey := GetConfig().OpsGenieConfig.AlertKey
		err := DeleteOpsGenieAlert(component, record.requestID, genieKey)
		if err != nil {
			Alert(fmt.Sprintf("Opsgenie remove incident error %v", err))
		}
	}
}

// CreateOpsGenieAlert creates an OpsGenie alert
func CreateOpsGenieAlert(msg Incident, genieKey string) error {
	Alert(fmt.Sprintf("report incident as pager escalation %v", msg))

	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	buf, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req, err := retryablehttp.NewRequest(http.MethodPost, opsGenieAlertURL, bytes.NewBuffer(buf))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", genieKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	log.Print("opsgenie status code ", resp.StatusCode)
	if resp.StatusCode > 300 {
		return fmt.Errorf("Opsgenie returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	alertResp := OpsGenieAlertResponse{}
	err = json.Unmarshal(bodyBytes, &alertResp)
	if err != nil {
		return err
	}

	incidents[msg.Entity] = incidentRecord{
		requestID: alertResp.RequestID,
		createdAt: time.Now(),
	}
	return nil
}

// DeleteOpsGenieAlert deletes an OpsGenie alert
// TODO this is not being used since we have no concrete scenario how to clear alerts
func DeleteOpsGenieAlert(component, requestID string, genieKey string) error {
	client := retryablehttp.NewClient()
	client.HTTPClient.Timeout = time.Duration(5) * time.Second
	client.RetryWaitMin = 4 * time.Second
	client.RetryWaitMax = 64 * time.Second
	client.RetryMax = 2

	deletionURL := fmt.Sprintf("%s/%s", opsGenieAlertURL, requestID)

	req, err := retryablehttp.NewRequest(http.MethodDelete, deletionURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", genieKey)
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}

	log.Print("opsgenie status code ", resp.StatusCode)
	if resp.StatusCode > 300 {
		return fmt.Errorf("Opsgenie returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	alertResp := OpsGenieAlertResponse{}
	err = json.Unmarshal(bodyBytes, &alertResp)
	if err != nil {
		return err
	}

	return nil
}
