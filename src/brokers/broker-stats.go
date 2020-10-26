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

const (
	topicStatsDBTable = "topic-stats"
)

// GetBrokers gets a list of brokers and ports
func GetBrokers(restBaseURL, clusterName, token string) ([]string, error) {
	brokersURL := util.SingleSlashJoin(restBaseURL, "admin/v2/brokers/"+clusterName)
	newRequest, err := http.NewRequest(http.MethodGet, brokersURL, nil)
	if err != nil {
		return nil, err
	}
	newRequest.Header.Add("user-agent", "pulsar-monitor")
	newRequest.Header.Add("Authorization", "Bearer "+token)
	client := &http.Client{
		CheckRedirect: util.PreserveHeaderForRedirect,
	}
	resp, err := client.Do(newRequest)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode > 300 {
		return nil, fmt.Errorf("failed to get a list of brokers, returns incorrect status code %d", resp.StatusCode)
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	brokers := []string{}
	err = json.Unmarshal(bodyBytes, &brokers)
	if err != nil {
		return nil, err
	}

	return brokers, nil
}

// required configuration cluster name and broker url
// 1. get a list of broker ips
// 2. query each broker individually
//

// BrokerTopicsQuery returns a map of broker and topic full name, or error of this operation
func BrokerTopicsQuery(brokerBaseURL, token string) ([]string, error) {
	// key is tenant, value is partition topic name
	if !strings.HasPrefix(brokerBaseURL, "http") {
		brokerBaseURL = "http://" + brokerBaseURL
	}
	topicStatsURL := util.SingleSlashJoin(brokerBaseURL, "admin/v2/broker-stats/topics")
	statsLog.Debugf(" proxy request route is %s\n", topicStatsURL)

	newRequest, err := http.NewRequest(http.MethodGet, topicStatsURL, nil)
	if err != nil {
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
		return nil, err
	}

	if response.StatusCode != http.StatusOK {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		statsLog.Errorf("GET broker topic stats request %s error %v", topicStatsURL, err)
		return nil, err
	}

	var namespaces map[string]map[string]map[string]map[string]interface{}
	if err = json.Unmarshal(body, &namespaces); err != nil {
		return nil, err
	}

	topics := []string{}
	for k, v := range namespaces {
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
	return topics, nil
}

// BrokerHealthCheck calls broker's health endpoint
func BrokerHealthCheck(broker, token string) error {
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
func TestBrokers(urlPrefix, clusterName, token string) (int, error) {
	brokers, err := GetBrokers(urlPrefix, clusterName, token)
	if err != nil {
		return 0, err
	}

	log.Debugf("got a list of brokers %v", brokers)
	failedBrokers := 0
	errStr := ""
	for _, brokerURL := range brokers {
		// check the broker health
		if err := BrokerHealthCheck(brokerURL, token); err != nil {
			errStr = errStr + ";;" + err.Error()
			failedBrokers++
			continue
		}
		log.Debugf("broker %s health ok", brokerURL)

		// Get broker topic stats
		topics, err := BrokerTopicsQuery(brokerURL, token)
		if err != nil {
			errStr = errStr + ";;" + err.Error()
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
				errStr = errStr + ";;" + err.Error()
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
		statsLog.Infof("broker %s health monitor required %d topic stats test, failed %d test", brokerURL, count, failureCount)
	}

	statsLog.Infof("cluster %s has %d failed brokers out of total %d brokers", clusterName, failedBrokers, len(brokers))
	if errStr != "" {
		return failedBrokers, errors.Errorf(errStr)
	}

	return failedBrokers, nil
}
