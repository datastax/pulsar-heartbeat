package metering

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/prometheus/client_golang/prometheus"
)

//Usage is the data usage per single tenant
type Usage struct {
	Name             string    `json:"name"`
	TotalMessagesIn  uint64    `json:"totalMessagesIn"`
	TotalBytesIn     uint64    `json:"totalBytesIn"`
	TotalMessagesOut uint64    `json:"totalMessagesOut"`
	TotalBytesOut    uint64    `json:"totalBytesOut"`
	MsgInBacklog     uint64    `json:"msgInBacklog"`
	UpdatedAt        time.Time `json:"updatedAt"`
}

// Usages the array usage that returns from the burnell usage query
type Usages []Usage

// TenantsUsage manages the tenant usage metering
type TenantsUsage struct {
	tenantLatestUsage map[string]Usage
	burnellURL        string
	token             string
	cluster           string
	isInitialized     bool
	messageInGauge    *prometheus.GaugeVec
	bytesInGauge      *prometheus.GaugeVec
	messageOutGauge   *prometheus.GaugeVec
	bytesOutGauge     *prometheus.GaugeVec
}

const (
	// Prometheus gauge type
	messagesIn30sGaugeType  = "msg_in_30s"
	bytesIn30sGaugeType     = "bytes_in_30s"
	messagesOut30sGaugeType = "msg_out_30s"
	bytesOut30sGaugeType    = "bytes_out_30s"

	// SamplingIntervalInSeconds - interval is 30 seconds
	// it is important that Prometheus scraping must be set to 30 seconds the same as this samping interval
	SamplingIntervalInSeconds = 30
)

// NewTenantsUsage creates a TenantsUsage
func NewTenantsUsage(url, pulsarToken, clusterName string) *TenantsUsage {
	return &TenantsUsage{
		tenantLatestUsage: make(map[string]Usage),
		burnellURL:        url,
		token:             pulsarToken,
		cluster:           clusterName,
		messageInGauge:    createPromGaugeVec(messagesIn30sGaugeType, "Plusar tenant total number of message in 30s"),
		bytesInGauge:      createPromGaugeVec(bytesIn30sGaugeType, "Plusar tenant total number of bytes for message in 30s"),
		messageOutGauge:   createPromGaugeVec(messagesOut30sGaugeType, "Plusar tenant total number of message out 30s"),
		bytesOutGauge:     createPromGaugeVec(bytesOut30sGaugeType, "Plusar tenant total number of bytes for message out 30s"),
	}
}

// TenantMessageByteOutOpt is the description for a tenant's total number of bytes for message out
func createPromGaugeVec(name, description string) *prometheus.GaugeVec {
	metric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pulsar",
			Subsystem: "tenant",
			Name:      name,
			Help:      description,
		},
		[]string{
			// device is the Pulsar cluster name
			"device",
			// tenant name for the usage
			"tenant",
		},
	)
	prometheus.MustRegister(metric)
	return metric
}

// PromGauge registers gauge reading
func (t *TenantsUsage) PromGauge(gaugeType, tenant string, num uint64) {
	switch gaugeType {
	case messagesIn30sGaugeType:
		t.messageInGauge.WithLabelValues(t.cluster, tenant).Set(float64(num))
	case bytesIn30sGaugeType:
		t.bytesInGauge.WithLabelValues(t.cluster, tenant).Set(float64(num))
	case messagesOut30sGaugeType:
		t.messageOutGauge.WithLabelValues(t.cluster, tenant).Set(float64(num))
	case bytesOut30sGaugeType:
		t.bytesOutGauge.WithLabelValues(t.cluster, tenant).Set(float64(num))
	default:
		log.Fatalf("unplanned gauge message type - %f", gaugeType)
	}
}

func getTenantStats(burnellURL, token string) (Usages, error) {
	// key is tenant, value is partition topic name
	if !strings.HasPrefix(burnellURL, "http") {
		burnellURL = "http://" + burnellURL
	}
	usageURL := util.SingleSlashJoin(burnellURL, "tenantsusage")

	newRequest, err := http.NewRequest(http.MethodGet, usageURL, nil)
	if err != nil {
		log.Errorf("make http request %s error %v", usageURL, err)
		return nil, err
	}
	newRequest.Header.Add("user-agent", "pulsar-monitor")
	newRequest.Header.Add("Authorization", "Bearer "+token)
	client := &http.Client{
		CheckRedirect: util.PreserveHeaderForRedirect,
	}
	response, err := client.Do(newRequest)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		log.Errorf("make http request %s error %v", usageURL, err)
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		log.Errorf("GET broker topic stats %s response status code %d", usageURL, response.StatusCode)
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("GET broker topic stats request %s error %v", usageURL, err)
		return nil, err
	}

	var usages Usages
	if err = json.Unmarshal(body, &usages); err != nil {
		log.Errorf("GET broker topic stats request %s unmarshal error %v", usageURL, err)
		return nil, err
	}
	return usages, nil

}

// UpdateUsages computes the usage by comparing with the last use
func (t *TenantsUsage) UpdateUsages() {
	usages, err := getTenantStats(t.burnellURL, t.token)
	if err != nil {
		log.Fatalf("failed to get burnell tenants' usage %v", err)
	}
	log.Infof("tenants usage %v", usages)

	// build the latest tenant usage
	for _, u := range usages {
		lastUsage, _ := t.tenantLatestUsage[u.Name]
		if t.isInitialized {
			t.PromGauge(messagesIn30sGaugeType, u.Name, util.ComputeDelta(lastUsage.TotalMessagesIn, u.TotalMessagesIn, 0))
			t.PromGauge(bytesIn30sGaugeType, u.Name, util.ComputeDelta(lastUsage.TotalBytesIn, u.TotalBytesIn, 0))
			t.PromGauge(messagesOut30sGaugeType, u.Name, util.ComputeDelta(lastUsage.TotalMessagesOut, u.TotalMessagesOut, 0))
			t.PromGauge(bytesOut30sGaugeType, u.Name, util.ComputeDelta(lastUsage.TotalBytesOut, u.TotalBytesOut, 0))
		}
		t.tenantLatestUsage[u.Name] = u
	}
	t.isInitialized = true
}
