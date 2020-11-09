package cfg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	insights "github.com/newrelic/go-insights/client"
)

type ampPayload struct {
	APIKey string     `json:"api_key"`
	Events []AmpEvent `json:"events"`
}

// AmpEvent the analytic event
type AmpEvent struct {
	UserID             string                 `json:"user_id,omitempty"`
	DeviceID           string                 `json:"device_id,omitempty"`
	EventType          string                 `json:"event_type,omitempty"`
	EventID            int                    `json:"event_id,omitempty"`
	SessionID          int64                  `json:"session_id,omitempty"`
	InsertID           string                 `json:"insert_id,omitempty"` // for dedupe
	EpochTime          int64                  `json:"time,omitempty"`
	EventProperties    map[string]interface{} `json:"event_properties,omitempty"`
	UserProperties     map[string]interface{} `json:"user_properties,omitempty"`
	AppVersion         string                 `json:"app_version,omitempty"`
	Platform           string                 `json:"platform,omitempty"`
	OSName             string                 `json:"os_name,omitempty"`
	OSVersion          string                 `json:"os_version,omitempty"`
	DeviceBrand        string                 `json:"device_brand,omitempty"`
	DeviceManufacturer string                 `json:"device_manufacturer,omitempty"`
	DeviceModel        string                 `json:"device_model,omitempty"`
	DeviceType         string                 `json:"device_type,omitempty"`
	Carrier            string                 `json:"carrier,omitempty"`
	Country            string                 `json:"country,omitempty"`
	Region             string                 `json:"region,omitempty"`
	City               string                 `json:"city,omitempty"`
	DMA                string                 `json:"dma,omitempty"`
	Language           string                 `json:"language,omitempty"`
	Revenue            float64                `json:"revenue,omitempty"`
	RevenueType        string                 `json:"revenueType,omitempty"`
	Latitude           float64                `json:"location_lat,omitempty"`
	Longitude          float64                `json:"location_lng,omitempty"`
	IP                 string                 `json:"ip,omitempty"`
	IDFA               string                 `json:"idfa,omitempty"`
	ADID               string                 `json:"adid,omitempty"`
}

// AppStartEvent event
type AppStartEvent struct {
	EventType string    `json:"eventType"`
	Timestamp time.Time `json:"timestamp"`
	Cluster   string    `json:"cluster"`
	AppName   string    `json:"name"`
	Env       string    `json:"env"`
}

// ReportIncidentEvent event
type ReportIncidentEvent struct {
	EventType   string    `json:"eventType"`
	Timestamp   time.Time `json:"timestamp"`
	Cluster     string    `json:"cluster"`
	Name        string    `json:"name"`
	Env         string    `json:"env"`
	ReportedBy  string    `json:"reportedBy"`
	Alias       string    `json:"alias"`
	Masseage    string    `json:"message"`
	Description string    `json:"description"`
}

// ClearIncidentEvent event
type ClearIncidentEvent struct {
	EventType       string    `json:"eventType"`
	Timestamp       time.Time `json:"timestamp"`
	Cluster         string    `json:"cluster"`
	Env             string    `json:"env"`
	ReportedBy      string    `json:"reportedBy"`
	DowntimeSeconds int       `json:"downtimeSeconds"`
}

// LatencyReportEvent event
type LatencyReportEvent struct {
	EventType           string    `json:"eventType"`
	Timestamp           time.Time `json:"timestamp"`
	Cluster             string    `json:"cluster"`
	Name                string    `json:"name"`
	Env                 string    `json:"env"`
	LatencyMs           int       `json:"latencyMs"`
	InOrderDelivery     bool      `json:"inOrderDelivery"`
	WithinLatencyBudget bool      `json:"withinLatencyBudget"`
	ErrorMessage        string    `json:"errorMessage"`
}

// HeartbeatEvent event
type HeartbeatEvent struct {
	EventType string    `json:"eventType"`
	Timestamp time.Time `json:"timestamp"`
	Cluster   string    `json:"cluster"`
	Env       string    `json:"env"`
}

// DowntimeReportEvent event
type DowntimeReportEvent struct {
	EventType       string    `json:"eventType"`
	Timestamp       time.Time `json:"timestamp"`
	Cluster         string    `json:"cluster"`
	DowntimeSeconds int       `json:"downtimeSeconds"`
	Env             string    `json:"env"`
}

const (
	// event name
	reportIncident = "Report Incident"
	clearIncident  = "Clear Incident"
	appStart       = "App Start"
	latencyReport  = "Latency Report"
	heartBeat      = "Heartbeat"
	downtimeReport = "Downtime Report"
)

var client *insights.InsertClient
var env string

// SetupAnalytics initializes and validates the configuration
func SetupAnalytics() {
	env = util.AssignString(os.Getenv("DeployEnv"), "testing")
	if GetConfig().AnalyticsConfig.InsightsWriteKey == "" || GetConfig().AnalyticsConfig.InsightsAccountID == "" {
		return
	}
	client = insights.NewInsertClient(GetConfig().AnalyticsConfig.InsightsWriteKey, GetConfig().AnalyticsConfig.InsightsAccountID)
	if err := client.Validate(); err != nil {
		log.Printf("insights client valiation failed with error %v", err)
		client = nil
		return
	}
}

func sendEvent(eventType, userID, deviceID string, eventProp map[string]interface{}) error {
	apiKey := GetConfig().AnalyticsConfig.APIKey
	ingestURL := GetConfig().AnalyticsConfig.IngestionURL
	if apiKey == "" || ingestURL == "" {
		return fmt.Errorf("no api key set up for analytics config")
	}

	headers := map[string][]string{
		"Content-Type": {"application/json"},
		"Accept":       {"*/*"},
	}
	epoch := time.Now().UnixNano()
	eventID := int(epoch)

	eventProp["deployEnv"] = env
	event := AmpEvent{
		UserID:          userID,
		DeviceID:        deviceID,
		EventType:       eventType,
		InsertID:        deviceID + strconv.Itoa(eventID),
		SessionID:       -1,
		EpochTime:       epoch,
		EventProperties: eventProp,
		Platform:        "k8s",
	}
	payload := ampPayload{
		APIKey: apiKey,
		Events: []AmpEvent{event},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, _ := http.NewRequest(http.MethodPost, ingestURL, bytes.NewBuffer(data))
	req.Header = headers

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	log.Print("amp analytics status code ", resp.StatusCode)
	if resp.StatusCode > 300 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		log.Println(buf.String())
		return fmt.Errorf("analytics endpoint returns failure status code %d", resp.StatusCode)
	}
	return nil

}

// sendToInsights send an event to insights
func sendToInsights(data interface{}) {
	if client == nil {
		return
	}
	if err := client.PostEvent(data); err != nil {
		log.Printf("failed to send to insights with error: %v\n", err)
	}
}

// AnalyticsReportIncident reports the beginning of an incident
func AnalyticsReportIncident(deviceID, alias, message, description string) {
	go sendEvent(reportIncident, deviceID, deviceID, map[string]interface{}{
		"cluster":     deviceID,
		"alias":       alias,
		"message":     message,
		"description": description,
		"reportedBy":  "pulsar monitor",
		"timestamp":   time.Now(),
	})

	go sendToInsights(ReportIncidentEvent{
		EventType:   reportIncident,
		Timestamp:   time.Now(),
		Cluster:     deviceID,
		Name:        deviceID,
		Env:         env,
		ReportedBy:  "pulsar monitor",
		Alias:       alias,
		Masseage:    message,
		Description: description,
	})
}

// AnalyticsClearIncident reports the end of an incident
func AnalyticsClearIncident(deviceID string, downtimeSeconds int) {
	go sendEvent(clearIncident, deviceID, deviceID, map[string]interface{}{
		"cluster":         deviceID,
		"reportedBy":      "pulsar monitor",
		"timestamp":       time.Now(),
		"downtimeSeconds": downtimeSeconds,
	})

	go sendToInsights(ClearIncidentEvent{
		EventType:       clearIncident,
		Timestamp:       time.Now(),
		Cluster:         deviceID,
		Env:             env,
		ReportedBy:      "pulsar monitor",
		DowntimeSeconds: downtimeSeconds,
	})
}

// AnalyticsAppStart reports a monitor starts
func AnalyticsAppStart(deviceID string) {
	go sendEvent(appStart, deviceID, deviceID, map[string]interface{}{
		"cluster":   deviceID,
		"name":      "pulsar monitor",
		"timestamp": time.Now(),
	})

	go sendToInsights(AppStartEvent{
		EventType: appStart,
		Timestamp: time.Now(),
		Cluster:   deviceID,
		Env:       env,
		AppName:   "pulsar monitor",
	})

	go sendToInsights(ClearIncidentEvent{
		EventType:       clearIncident,
		Timestamp:       time.Now(),
		Cluster:         deviceID,
		Env:             env,
		ReportedBy:      "app start",
		DowntimeSeconds: 0,
	})
}

// AnalyticsLatencyReport reports a monitor starts
func AnalyticsLatencyReport(deviceID, name, errorMessage string, latency int, inOrderDelivery, withinLatencyBudget bool) {
	go sendEvent(latencyReport, deviceID, deviceID, map[string]interface{}{
		"cluster":             deviceID,
		"catetory":            "pulsar pub sub latency",
		"name":                name,
		"latencyMs":           latency,
		"timestamp":           time.Now(),
		"inOrderDelivery":     inOrderDelivery,
		"withinLatencyBudget": withinLatencyBudget,
		"error":               errorMessage,
	})

	go sendToInsights(LatencyReportEvent{
		EventType:           latencyReport,
		Timestamp:           time.Now(),
		Cluster:             deviceID,
		Name:                name,
		Env:                 env,
		LatencyMs:           latency,
		InOrderDelivery:     inOrderDelivery,
		WithinLatencyBudget: withinLatencyBudget,
		ErrorMessage:        errorMessage,
	})
}

// AnalyticsHeartbeat reports heartbeat
func AnalyticsHeartbeat(deviceID string) {
	go sendToInsights(HeartbeatEvent{
		EventType: heartBeat,
		Timestamp: time.Now(),
		Cluster:   deviceID,
		Env:       env,
	})
}

// AnalyticsDowntime reports downtime
func AnalyticsDowntime(deviceID string, downtimeSeconds int) {
	go sendToInsights(DowntimeReportEvent{
		EventType:       downtimeReport,
		Timestamp:       time.Now(),
		Cluster:         deviceID,
		Env:             env,
		DowntimeSeconds: downtimeSeconds,
	})
}
