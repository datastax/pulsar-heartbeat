package topic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
	"github.com/prometheus/common/log"
	logrus "github.com/sirupsen/logrus"
)

// partition topics can be used to test availabilities of all PartitionTopic

// PartitionTopics data struct is the persistent partition topic name and number of partitions it has
type PartitionTopics struct {
	NumberOfPartitions int
	PulsarURL          string
	Token              string
	TrustStore         string
	Tenant             string
	Namespace          string
	PartitionTopicName string
	TopicFullname      string
	BaseAdminURL       string
	log                *logrus.Entry
}

// NewPartitionTopic creates a PartitionTopic test object
func NewPartitionTopic(url, token, trustStore, topicFn, adminURL string, numOfPartitions int) (*PartitionTopics, error) {
	isPersistent, tenant, ns, topic, err := util.TokenizeTopicFullName(topicFn)
	if err != nil {
		return nil, err
	}
	if !isPersistent {
		return nil, fmt.Errorf("does not support non-persistent topic in partition topic test")
	}
	return &PartitionTopics{
		NumberOfPartitions: numOfPartitions,
		PulsarURL:          url,
		Token:              token,
		TrustStore:         trustStore,
		Tenant:             tenant,
		Namespace:          ns,
		PartitionTopicName: topic,
		TopicFullname:      topicFn,
		BaseAdminURL:       adminURL,
		log:                logrus.WithFields(logrus.Fields{"app": "partition topic test"}),
	}, nil
}

// GetPartitionTopic gets the partition topic
func (pt *PartitionTopics) GetPartitionTopic() (bool, error) {
	url := pt.BaseAdminURL + "/admin/v2/peristent/" + pt.Tenant + "/" + pt.Namespace + "/partitioned"

	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return false, nil
	}

	request.Header.Add("Authorization", "Bearer "+pt.Token)
	client := &http.Client{}
	response, err := client.Do(request)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		pt.log.Errorf("GET PartitionTopic %s error %v", url, err)
		return false, err
	}

	if response.StatusCode != http.StatusOK {
		pt.log.Errorf("GET PartitionTopic %s response status code %d", url, response.StatusCode)
		return false, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		pt.log.Errorf("GET PartitionTopic %s read response body error %v", url, err)
		return false, err
	}
	var partitionTopic []string
	if err = json.Unmarshal(body, &partitionTopic); err != nil {
		pt.log.Errorf("GET PartitionTopic %s unmarshal response body error %v", url, err)
		return false, err
	}
	expectedTopic := "persistent://" + pt.Tenant + "/" + pt.Namespace + "/" + pt.PartitionTopicName
	found := false
	for _, v := range partitionTopic {
		if expectedTopic == v {
			found = true
		}
	}

	return found, nil
}

// CreatePartitionTopic creates a partition topic
func (pt *PartitionTopics) CreatePartitionTopic() error {
	url := pt.BaseAdminURL + "/admin/v2/peristent/" + pt.Tenant + "/" + pt.Namespace + "/" + pt.PartitionTopicName

	byteInt := []byte(strconv.Itoa(pt.NumberOfPartitions))
	request, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(byteInt))
	if err != nil {
		return nil
	}

	request.Header.Add("Authorization", "Bearer "+pt.Token)
	client := &http.Client{}
	response, err := client.Do(request)
	if response != nil {
		defer response.Body.Close()
	}
	if err != nil {
		pt.log.Errorf("GET PartitionTopic %s error %v", url, err)
		return err
	}

	if response.StatusCode != http.StatusOK {
		pt.log.Errorf("GET PartitionTopic %s response status code %d", url, response.StatusCode)
		return err
	}

	return nil
}

// TestPartitionTopic sends multiple messages and to be verified by multiple consumers
func (pt *PartitionTopics) TestPartitionTopic() (time.Duration, error) {

	// notify the main thread with the latency to complete the exit of all consumers
	completeChan := make(chan *util.ConsumerResult, pt.NumberOfPartitions)

	partitionTopicSuffix := "-partition-"

	client, err := util.GetPulsarClient(pt.PulsarURL, pt.Token, pt.TrustStore)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	pt.log.Infof("create a topic producer %s", pt.TopicFullname)
	// create a pulsar producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: pt.TopicFullname,
	})
	if err != nil {
		return 0, err
	}
	defer producer.Close()

	// prepare the message
	message := fmt.Sprintf("partition topic test message %v", time.Now())

	// start multiple consumers and listens to individual partition topics
	for i := 0; i < pt.NumberOfPartitions; i++ {
		topicName := pt.TopicFullname + partitionTopicSuffix + strconv.Itoa(i)
		pt.log.Infof("subscribe to partition topic %s wait on message %s", topicName, message)
		go util.VerifyMessageByPulsarConsumer(client, topicName, message, completeChan)
	}

	// producer sends multiple messages
	start := time.Now()
	for i := 0; i < pt.NumberOfPartitions; i++ {
		ctx := context.Background()

		// Create a different message to send asynchronously
		msg := pulsar.ProducerMessage{
			Payload: []byte(message),
			Key:     "partitionkey" + strconv.Itoa(i),
		}

		// Attempt to send message asynchronously and handle the response
		if _, err := producer.Send(ctx, &msg); err != nil {
			log.Errorf("failed to send message over partition topic , error: %v", err)
			errMsg := fmt.Sprintf("fail to instantiate Pulsar client: %v", err)
			// report error and exit
			completeChan <- &util.ConsumerResult{
				Err: errors.New(errMsg),
			}
		}

		log.Infof("successfully published message on topic %s ", pt.TopicFullname)
	}

	receivedCounter := 0
	successfulCounter := 0
	ticker := time.NewTicker(90 * time.Second)
	defer ticker.Stop()
	for receivedCounter < pt.NumberOfPartitions {
		select {
		case signal := <-completeChan:
			receivedCounter++
			log.Infof(" received counter %d", receivedCounter)
			if signal.Err != nil {
				log.Errorf("topic %s receive error: %v", pt.TopicFullname, signal.Err)
			} else if signal.InOrderDelivery {
				successfulCounter++
				log.Infof("successfully received counter %d", successfulCounter)
			} else {
				log.Errorf("topic %s failed to receive expected messages", pt.TopicFullname)
			}
			if receivedCounter >= pt.NumberOfPartitions {
				return time.Since(start), nil
			}
		case <-ticker.C:
			return 0, fmt.Errorf("timed out to receive message %d out of %d partition topics", receivedCounter, pt.NumberOfPartitions)
		}
	}
	return 0, nil
}
