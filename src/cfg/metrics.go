package cfg

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/kafkaesque-io/pulsar-monitor/src/metering"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metrics   = make(map[string]*prometheus.GaugeVec)
	summaries = make(map[string]*prometheus.SummaryVec)
	counters  = make(map[string]*prometheus.CounterVec)
)

const (
	funcTopicSubsystem     = "func_topic"
	pubSubSubsystem        = "pubsub"
	websocketSubsystem     = "websocket"
	heartbeatSubsystem     = "heartbeat"
	downtimeSubsystem      = "downtime"
	k8sBrokerSubsystem     = "k8s_broker"
	k8sBookkeeperSubsystem = "k8s_bookkeeper"
	k8sZookeeperSubsystem  = "k8s_zookeeper"
	k8sProxySubsystem      = "k8s_proxy"
	k8sUndefinedSubsystem  = "k8s_undefined"
)

// This is Premetheus data modelling and naming convention
// https://prometheus.io/docs/practices/naming/
// https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
// TODO add regex evaluation against names [a-zA-Z_:][a-zA-Z0-9_:]*

// TenantsGaugeOpt is the description for rest api tenant counts
func TenantsGaugeOpt() prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "pulsar",
		Subsystem: "tenant",
		Name:      "size",
		Help:      "Plusar rest api tenant counts",
	}
}

// OfflinePodGaugeOpt is offline pods counter
func OfflinePodGaugeOpt(subsystem, desc string) prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "pulsar",
		Subsystem: subsystem,
		Name:      "offline_counter",
		Help:      desc,
	}
}

// SiteLatencyGaugeOpt is the description for hosting site latency gauge
func SiteLatencyGaugeOpt() prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "website",
		Subsystem: "webendpoint",
		Name:      "latency_ms",
		Help:      "website endpoint monitor and latency in ms",
	}
}

// MsgLatencyGaugeOpt is the description for Pulsar message latency gauge
func MsgLatencyGaugeOpt(typeName, desc string) prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "pulsar",
		Subsystem: typeName,
		Name:      "latency_ms",
		Help:      desc,
	}
}

// HeartbeatCounterOpt is the description for heart beat counter
func HeartbeatCounterOpt() prometheus.CounterOpts {
	return prometheus.CounterOpts{
		Namespace: "pulsar",
		Subsystem: "monitor",
		Name:      "counter",
		Help:      "Pulsar cluster monitor heartbeat",
	}
}

// PubSubDowntimeGaugeOpt is the description for downtime summary
func PubSubDowntimeGaugeOpt() prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "pulsar",
		Subsystem: "pubsub",
		Name:      "downtime_seconds",
		Help:      "Pulsar pubsub downtime in seconds",
	}
}

// FuncLatencyGaugeOpt is the description of Pulsar Function latency gauge
func FuncLatencyGaugeOpt() prometheus.GaugeOpts {
	return prometheus.GaugeOpts{
		Namespace: "pulsar",
		Subsystem: "function",
		Name:      "latency_ms",
		Help:      "Plusar message latency in ms",
	}
}

// PromGaugeInt registers gauge reading in integer
func PromGaugeInt(opt prometheus.GaugeOpts, cluster string, num int) {
	PromGauge(opt, cluster, float64(num))
}

// PromGauge registers gauge reading
func PromGauge(opt prometheus.GaugeOpts, cluster string, num float64) {
	key := getMetricKey(opt)
	if promMetric, ok := metrics[key]; ok {
		promMetric.WithLabelValues(cluster).Set(num)
	} else {
		newMetric := prometheus.NewGaugeVec(opt, []string{"device"})
		prometheus.Register(newMetric)
		newMetric.WithLabelValues(cluster).Set(num)
		metrics[key] = newMetric
	}
}

// PromCounter registers counter and increment
func PromCounter(opt prometheus.CounterOpts, cluster string) {
	key := fmt.Sprintf("%s-%s-%s", opt.Namespace, opt.Subsystem, opt.Name)
	if promMetric, ok := counters[key]; ok {
		promMetric.WithLabelValues(cluster).Inc()
	} else {
		newMetric := prometheus.NewCounterVec(opt, []string{"device"})
		prometheus.Register(newMetric)
		newMetric.WithLabelValues(cluster).Inc()
		counters[key] = newMetric
	}
}

// PromLatencySum expose monitoring metrics to Prometheus
func PromLatencySum(opt prometheus.GaugeOpts, cluster string, latency time.Duration) {
	key := getMetricKey(opt)
	ms := float64(latency / time.Millisecond)
	if promMetric, ok := metrics[key]; ok {
		promMetric.WithLabelValues(cluster).Set(ms)
	} else {
		newMetric := prometheus.NewGaugeVec(opt, []string{"device"})
		prometheus.Register(newMetric)
		newMetric.WithLabelValues(cluster).Set(ms)
		metrics[key] = newMetric
	}

	if summary, ok := summaries[key]; ok {
		summary.WithLabelValues(cluster).Observe(ms)
	} else {
		newSummary := prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:  opt.Namespace,
			Subsystem:  opt.Subsystem,
			Name:       fmt.Sprintf("%s_hst", opt.Name),
			Help:       opt.Help,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			MaxAge:     30 * time.Minute,
			AgeBuckets: 3,
			BufCap:     500,
		}, []string{"device"})
		prometheus.MustRegister(newSummary)
		newSummary.WithLabelValues(cluster).Observe(ms)
		summaries[key] = newSummary
	}

}

func getMetricKey(opt prometheus.GaugeOpts) string {
	return fmt.Sprintf("%s-%s-%s", opt.Namespace, opt.Subsystem, opt.Name)
}

// GetGaugeType get the Prometheus Gauge Option based on type/subsystem
func GetGaugeType(nameType string) prometheus.GaugeOpts {
	if nameType == funcTopicSubsystem || strings.HasPrefix(nameType, "func_topic") {
		return MsgLatencyGaugeOpt(funcTopicSubsystem, "Plusar function input output topic latency in ms")
	}

	if nameType == websocketSubsystem {
		return MsgLatencyGaugeOpt(websocketSubsystem, "Plusar websocket pubsub topic latency in ms")
	}

	return MsgLatencyGaugeOpt(pubSubSubsystem, "Plusar pubsub message latency in ms")
}

// GetOfflinePodsCounter returns prometheus GaugeOpts for kubernetes cluster pod offline counter
func GetOfflinePodsCounter(subsystem string) prometheus.GaugeOpts {
	switch subsystem {
	case k8sBookkeeperSubsystem:
		return OfflinePodGaugeOpt(k8sBookkeeperSubsystem, "Pulsar k8s clueter bookkeeper pods offline counter")
	case k8sBrokerSubsystem:
		return OfflinePodGaugeOpt(k8sBrokerSubsystem, "Pulsar k8s clueter broker pods offline counter")
	case k8sProxySubsystem:
		return OfflinePodGaugeOpt(k8sProxySubsystem, "Pulsar k8s clueter proxy pods offline counter")
	case k8sZookeeperSubsystem:
		return OfflinePodGaugeOpt(k8sZookeeperSubsystem, "Pulsar k8s clueter zookeeper pods offline counter")
	default:
		return OfflinePodGaugeOpt(k8sUndefinedSubsystem, "Pulsar k8s clueter undefined pods offline counter")
	}
}

// scrapeLocal scrapes the local metrics
func scrapeLocal() ([]byte, error) {
	url := "http://localhost" + GetConfig().PrometheusConfig.Port + "/metrics"
	newRequest, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Errorf("make http request to scrape self's prometheus %s error %v", url, err)
		return []byte{}, err
	}
	client := &http.Client{}
	response, err := client.Do(newRequest)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		log.Errorf("scrape self's prometheus %s error %v", url, err)
		return []byte{}, err
	}

	if response.StatusCode != http.StatusOK {
		log.Errorf("scrape self's prometheus %s response status code %d", url, response.StatusCode)
		return []byte{}, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Errorf("scrape self's prometheus %s read response body error %v", url, err)
		return []byte{}, err
	}

	var rc string
	scanner := bufio.NewScanner(strings.NewReader(string(body)))

	pattern := fmt.Sprintf(`.*pulsar.*`)
	for scanner.Scan() {
		text := scanner.Text()
		matched, err := regexp.MatchString(pattern, text)
		if matched && err == nil {
			rc = fmt.Sprintf("%s%s\n", rc, text)
		}
	}
	return []byte(strings.TrimSuffix(rc, "\n")), nil
}

// PushToPrometheusProxy pushes exp data to PrometheusProxy
func PushToPrometheusProxy(proxyURL, authKey string) error {
	data, err := scrapeLocal()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", proxyURL, bytes.NewBuffer(data))
	if err != nil {
		log.Errorf("push to prometheus proxy %s error NewRe	uest request %v", proxyURL, err)
		return err
	}

	req.Header.Set("Authorization", authKey)

	client := &http.Client{Timeout: time.Second * 50}

	// Send request
	resp, err := client.Do(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		log.Errorf("push to prometheus proxy %s error reading request %v", proxyURL, err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("push to prometheus proxy %s error status code %v", proxyURL, resp.StatusCode)
		return fmt.Errorf("push to prometheus proxy %s error status code %v", proxyURL, resp.StatusCode)
	}

	return nil
}

// PushToPrometheusProxyThread is the daemon thread that scrape and pushes metrics to prometheus proxy
func PushToPrometheusProxyThread() {
	promCfg := GetConfig().PrometheusConfig
	if promCfg.PrometheusProxyURL == "" || !promCfg.ExposeMetrics {
		log.Infof("This process is not configured to push metrics to prometheus proxy.")
		return
	}
	proxyInstanceURL := promCfg.PrometheusProxyURL + "/" + GetConfig().Name

	log.Infof("push to prometheus proxy url %s %t", GetConfig().PrometheusConfig.PrometheusProxyURL, promCfg.ExposeMetrics)
	go func(url, apikey string) {
		ticker := time.NewTicker(10 * time.Second)
		PushToPrometheusProxy(url, apikey)
		for {
			select {
			case <-ticker.C:
				PushToPrometheusProxy(url, apikey)
			}
		}
	}(proxyInstanceURL, promCfg.PrometheusProxyAPIKey)

}

// BuildTenantsUsageThread is the daemon thread that builds last 30s tenants usage and expose to Prometheus metrics
func BuildTenantsUsageThread() {
	token := GetConfig().Token
	if token == "" {
		log.Errorf("tenants usage exits since no token is specified")
		return
	}

	prefixURL := GetConfig().BrokersConfig.InClusterRESTURL
	if prefixURL == "" {
		log.Errorf("tenants usage exits since no in-cluster REST URL prefix is specified")
		return
	}

	name := GetConfig().Name
	tenantBytesOutAlertLimit := GetConfig().TenantUsageConfig.OutBytesLimit
	interval := time.Duration(metering.SamplingIntervalInSeconds) * time.Second

	log.Infof("build tenants uages from %s", prefixURL)
	go func(url, jwt, cluster string) {
		ticker := time.NewTicker(interval)
		usage := metering.NewTenantsUsage(url, jwt, cluster, tenantBytesOutAlertLimit)
		usage.UpdateUsages()
		errStr := usage.ReportHighUsageTenant()
		if errStr != "" {
			Alert(errStr)
		}

		// monitor and report over limit tenant's usage
		go func() {
			alertInterval := util.TimeDuration(GetConfig().TenantUsageConfig.AlertIntervalMinutes, 120, time.Minute)
			usageAlertTicker := time.NewTicker(alertInterval)
			select {
			case <-usageAlertTicker.C:
				errStr := usage.ReportHighUsageTenant()
				if errStr != "" {
					Alert(errStr)
				}
			}

		}()

		// calculate tenant usage and send to prometheus
		for {
			select {
			case <-ticker.C:
				usage.UpdateUsages()
			}
		}
	}(prefixURL, token, name)
}
