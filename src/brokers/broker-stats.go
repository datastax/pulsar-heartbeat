package brokers

// monitor individual broker health
// monitor load balance, and the number of topics balance on broker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/pkg/errors"
)

var statsLog = log.WithFields(log.Fields{"app": "broker health monitor"})

// BrokerMonitor is the
type BrokerMonitor struct {
	RESTURL              string
	token                string
	ExpectedBrokerNumber int
}

const (
	topicStatsDBTable = "topic-stats"
)

// BrokerStats is per broker statistics
type BrokerStats struct {
	Broker string                                                  `json:"broker"`
	Data   map[string]map[string]map[string]map[string]interface{} `json:"data"`
}

// Stats is the json response object for REST API
type Stats struct {
	Total  int           `json:"total"`
	Offset int           `json:"offset"`
	Data   []BrokerStats `json:"data"`
}

// TopicStats is the usage for topic on each individual broker
type TopicStats struct {
	ID        string      `json:"id"` // ID is the topic fullname
	Tenant    string      `json:"tenant"`
	Namespace string      `json:"namespace"`
	Topic     string      `json:"topic"`
	Data      interface{} `json:"data"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

// brokersTopicsQuery returns a map of broker and topic full name, and error of this operation
func brokersTopicsQuery(urlString, token string) (map[string][]string, error) {
	// key is tenant, value is partition topic name
	var brokerTopicMap = make(map[string][]string)

	if !strings.HasPrefix(urlString, "http") {
		urlString = "http://" + urlString
	}
	topicStatsURL := util.SingleSlashJoin(urlString, "admin/v2/broker-stats/topics")
	statsLog.Debugf(" proxy request route is %s\n", topicStatsURL)

	newRequest, err := http.NewRequest(http.MethodGet, topicStatsURL, nil)
	if err != nil {
		statsLog.Errorf("make http request %s error %v", topicStatsURL, err)
		return brokerTopicMap, err
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
		statsLog.Errorf("make http request %s error %v", topicStatsURL, err)
		return brokerTopicMap, err
	}

	if response.StatusCode != http.StatusOK {
		statsLog.Errorf("GET broker topic stats %s response status code %d", topicStatsURL, response.StatusCode)
		return brokerTopicMap, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		statsLog.Errorf("GET broker topic stats request %s error %v", topicStatsURL, err)
		return brokerTopicMap, err
	}

	var brokers Stats
	if err = json.Unmarshal(body, &brokers); err != nil {
		statsLog.Errorf("GET broker topic stats request %s unmarshal error %v", topicStatsURL, err)
		return brokerTopicMap, err
	}

	for _, broker := range brokers.Data {
		var topics []string
		for k, v := range broker.Data {
			tenant := strings.Split(k, "/")[0]
			statsLog.Debugf("namespace %s tenant %s", k, tenant)

			for bundleKey, v2 := range v {
				statsLog.Debugf("  bundle %s", bundleKey)
				for persistentKey, v3 := range v2 {
					statsLog.Debugf("    %s key", persistentKey)

					for topicFn := range v3 {
						// statsLog.Infof("      topic name %s", topicFn)
						topics = append(topics, topicFn)
					}
				}
			}
		}
		brokerTopicMap[broker.Broker] = topics
	}

	return brokerTopicMap, nil
}

func brokerHealthCheck(broker, token string) error {
	// key is tenant, value is partition topic name
	if !strings.HasPrefix(broker, "http") {
		broker = "http://" + broker
	}
	brokerURL := util.SingleSlashJoin(broker, "admin/v2/brokers/health")

	newRequest, err := http.NewRequest(http.MethodGet, brokerURL, nil)
	if err != nil {
		return fmt.Errorf("broker healthcheck newRequest %s error %v", brokerURL, err)
	}
	newRequest.Header.Add("user-agent", "pulsar-monitor")
	newRequest.Header.Add("Authorization", "Bearer "+token)
	client := &http.Client{
		CheckRedirect: util.PreserveHeaderForRedirect,
		Timeout:       3 * time.Second,
	}
	response, err := client.Do(newRequest)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("broker healthcheck request %s error %v", brokerURL, err)
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET broker healthcheck %s response status code %d", brokerURL, response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("GET broker healthcheck %s error %v", brokerURL, err)
	}

	rc := string(body)
	if rc != "ok" {
		return fmt.Errorf("broker healthcheck %s status is not ok but %s", brokerURL, rc)
	}
	return nil
}

// QueryTopicStats query a single topic stats
func QueryTopicStats(url, token string) error {
	newRequest, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("make http request %s error %v", url, err)
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
		return fmt.Errorf("make http request %s error %v", url, err)
	}

	// log.Infof("status code %d", response.StatusCode)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET broker topic stats %s response status code %d", url, response.StatusCode)
	}

	return nil
}

// TestBrokers evaluates all brokers' health
func TestBrokers(urlPrefix, token string) (int, error) {
	brokerTopics, err := brokersTopicsQuery(urlPrefix, token)
	if err != nil {
		return 0, err
	}

	failedBrokers := 0
	errorStr := ""
	for brokerName, topics := range brokerTopics {
		if err := brokerHealthCheck(brokerName, token); err != nil {
			errorStr = errorStr + ";;" + err.Error()
			failedBrokers++
			continue
		}
		requiredCount := util.MinInt(3, len(topics)-1) //subtract the healthcheck topic
		count := 0
		failureCount := 0
		for _, topic := range topics {
			if strings.HasSuffix(topic, "/healthcheck") {
				continue
			}
			url, err := util.TopicFnToURL(topic)
			if err != nil {
				fmt.Printf("%v", err)
				continue
			}
			url = util.SingleSlashJoin(util.SingleSlashJoin(urlPrefix, "/admin/v2/"), url+"/stats")
			err = QueryTopicStats(url, token)
			if err != nil {
				errorStr = errorStr + ";;" + err.Error()
				failureCount++
			}
			count++

			if failureCount > 1 {
				failedBrokers++
				break
			}
			if count >= requiredCount {
				break
			}
		}
		statsLog.Infof("broker %s health monitor required %d topic stats test, failed %d test", brokerName, count, failureCount)
	}
	statsLog.Infof("failed %d brokers out of total %d brokers", failedBrokers, len(brokerTopics))
	if errorStr != "" {
		return failedBrokers, errors.Errorf(errorStr)
	}

	return failedBrokers, nil
}
