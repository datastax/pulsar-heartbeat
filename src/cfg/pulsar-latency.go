package cfg

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/apex/log"
	"github.com/kafkaesque-io/pulsar-monitor/src/topic"
	"github.com/kafkaesque-io/pulsar-monitor/src/util"
)

const (
	latencyBudget = 2400 // in Millisecond integer, will convert to time.Duration in evaluation
	failedLatency = 100 * time.Second
)

var (
	clients         = make(map[string]pulsar.Client)
	partitionTopics = make(map[string]*topic.PartitionTopics)
)

// MsgResult stores the result of message test
type MsgResult struct {
	InOrderDelivery bool
	Latency         time.Duration
	SentTime        time.Time
}

// GetPulsarClient gets the pulsar client object
// Note: the caller has to Close() the client object
func GetPulsarClient(pulsarURL, tokenStr string) (pulsar.Client, error) {
	client, ok := clients[pulsarURL]
	if !ok {
		clientOpt := pulsar.ClientOptions{
			URL:               pulsarURL,
			OperationTimeout:  30 * time.Second,
			ConnectionTimeout: 30 * time.Second,
		}

		if tokenStr != "" {
			clientOpt.Authentication = pulsar.NewAuthenticationToken(tokenStr)
		}

		if strings.HasPrefix(pulsarURL, "pulsar+ssl://") {
			trustStore := util.AssignString(GetConfig().TrustStore, "/etc/ssl/certs/ca-bundle.crt")
			if trustStore == "" {
				return nil, fmt.Errorf("fatal error: missing trustStore while pulsar+ssl tls is enabled")
			}
			clientOpt.TLSTrustCertsFilePath = trustStore
		}

		pulsarClient, err := pulsar.NewClient(clientOpt)
		if err != nil {
			return nil, err
		}
		clients[pulsarURL] = pulsarClient
		return pulsarClient, nil
	}
	return client, nil
}

// PubSubLatency the latency including successful produce and consume of a message
func PubSubLatency(clusterName, tokenStr, uri, topicName, outputTopic, msgPrefix, expectedSuffix string, payloads [][]byte, maxPayloadSize int) (MsgResult, error) {
	client, err := GetPulsarClient(uri, tokenStr)
	if err != nil {
		return MsgResult{Latency: failedLatency}, err
	}

	// it is important to close client after close of producer/consumer
	// defer client.Close()

	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	if err != nil {
		// we guess something could have gone wrong if producer cannot be created
		client.Close()
		delete(clients, uri)
		return MsgResult{Latency: failedLatency}, err
	}

	defer producer.Close()

	subscriptionName := "latency-measure"

	// use the same input topic if outputTopic does not exist
	// Two topic use case could be for Pulsar function test
	consumerTopic := util.AssignString(outputTopic, topicName)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       consumerTopic,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})

	if err != nil {
		defer client.Close() //must defer to allow producer to be closed first
		delete(clients, uri)
		return MsgResult{Latency: failedLatency}, err
	}
	defer consumer.Close()

	// notify the main thread with the latency to complete the exit
	completeChan := make(chan MsgResult, 1)

	// error report channel
	errorChan := make(chan error, 1)

	// payloadStr := "measure-latency123" + time.Now().Format(time.UnixDate)
	receivedCount := len(payloads)

	// Key is payload in string, value is pointer to a MsgResult
	sentPayloads := make(map[string]*MsgResult, receivedCount)
	// Use mutex instead of sync.Map in favour of performance and simplicity
	//  and because no need to protect map iteration to calculate results
	mapMutex := &sync.Mutex{}

	receiveTimeout := util.TimeDuration(5+(maxPayloadSize/102400), 10, time.Second)
	go func() {

		lastMessageIndex := -1 // to track the message delivery order
		for receivedCount > 0 {
			cCtx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
			defer cancel()

			log.Infof("wait to receive on message count %d", receivedCount)
			msg, err := consumer.Receive(cCtx)
			if err != nil {
				receivedCount = 0 // play safe?
				errorChan <- fmt.Errorf("consumer Receive() error: %v", err)
				break
			}
			receivedTime := time.Now()
			receivedStr := string(msg.Payload())
			currentMsgIndex := GetMessageID(msgPrefix, receivedStr)

			mapMutex.Lock()
			result, ok := sentPayloads[receivedStr]
			mapMutex.Unlock()
			if ok {
				receivedCount--
				result.Latency = receivedTime.Sub(result.SentTime)
				if currentMsgIndex > lastMessageIndex {
					result.InOrderDelivery = true
					lastMessageIndex = currentMsgIndex
				}
			}
			consumer.Ack(msg)
			log.Infof("consumer received message index %d payload size %d\n", currentMsgIndex, len(receivedStr))
		}

		//successful case all message received
		if receivedCount == 0 {
			var total time.Duration
			inOrder := true
			for _, v := range sentPayloads {
				total += v.Latency
				inOrder = inOrder && v.InOrderDelivery
			}

			// receiverLatency <- total / receivedCount
			completeChan <- MsgResult{
				Latency:         time.Duration(int(total/time.Millisecond)/len(payloads)) * time.Millisecond,
				InOrderDelivery: inOrder,
			}
		}

	}()

	for _, payload := range payloads {
		ctx := context.Background()

		// Create a different message to send asynchronously
		asyncMsg := pulsar.ProducerMessage{
			Payload: payload,
		}

		sentTime := time.Now()
		expectedMsg := expectedMessage(string(payload), expectedSuffix)
		mapMutex.Lock()
		sentPayloads[expectedMsg] = &MsgResult{SentTime: sentTime}
		mapMutex.Unlock()
		// Attempt to send message asynchronously and handle the response
		producer.SendAsync(ctx, &asyncMsg, func(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
			if err != nil {
				errMsg := fmt.Sprintf("fail to instantiate Pulsar client: %v", err)
				log.Infof(errMsg)
				// report error and exit
				errorChan <- errors.New(errMsg)
			}

			log.Infof("successfully published %v", sentTime)
			CalculateDowntime(clusterName)
		})
	}

	ticker := time.NewTicker(time.Duration(5*len(payloads)) * time.Second)
	defer ticker.Stop()
	select {
	case receiverLatency := <-completeChan:
		return receiverLatency, nil
	case reportedErr := <-errorChan:
		log.Infof("received error %v", reportedErr)
		return MsgResult{Latency: failedLatency}, reportedErr
	case <-ticker.C:
		return MsgResult{Latency: failedLatency}, errors.New("latency measure not received after timeout")
	}
}

// TopicLatencyTestThread tests a message delivery in topic and measure the latency.
func TopicLatencyTestThread() {
	topics := GetConfig().PulsarTopicConfig
	log.Infof("topic configuration %v", topics)

	for _, topic := range topics {
		go func(t TopicCfg) {
			ticker := time.NewTicker(util.TimeDuration(t.IntervalSeconds, 60, time.Second))
			TestTopicLatency(t)
			for {
				select {
				case <-ticker.C:
					TestTopicLatency(t)
				}
			}
		}(topic)
	}
}

// TestTopicLatency test generic message delivery in topics and the latency
func TestTopicLatency(topicCfg TopicCfg) {
	// uri is in the form of pulsar+ssl://fqdn:6651
	adminURL, err := url.ParseRequestURI(topicCfg.PulsarURL)
	if err != nil {
		panic(err) //panic because this is a showstopper
	}
	clusterName := adminURL.Hostname()
	token := util.AssignString(topicCfg.Token, GetConfig().Token)

	if topicCfg.NumberOfPartitions < 2 {
		testTopicLatency(clusterName, token, topicCfg)
	} else {
		testPartitionTopic(clusterName, token, topicCfg)
	}
}

func testTopicLatency(clusterName, token string, topicCfg TopicCfg) {
	stdVerdict := util.GetStdBucket(clusterName)
	expectedLatency := util.TimeDuration(topicCfg.LatencyBudgetMs, latencyBudget, time.Millisecond)
	prefix := "messageid"
	payloads, maxPayloadSize := AllMsgPayloads(prefix, topicCfg.PayloadSizes, topicCfg.NumOfMessages)
	log.Infof("send %d messages to topic %s on cluster %s with latency budget %v, %v, %d",
		len(payloads), topicCfg.TopicName, topicCfg.PulsarURL, expectedLatency, topicCfg.PayloadSizes, topicCfg.NumOfMessages)
	result, err := PubSubLatency(clusterName, token, topicCfg.PulsarURL, topicCfg.TopicName, topicCfg.OutputTopic, prefix, topicCfg.ExpectedMsg, payloads, maxPayloadSize)

	testName := util.AssignString(topicCfg.Name, pubSubSubsystem)
	log.Infof("cluster %s has message latency %v", clusterName, result.Latency)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s latency test Pulsar error: %v", clusterName, testName, err)
		VerboseAlert(clusterName+"-latency-err", errMsg, 3*time.Minute)
		ReportIncident(clusterName, clusterName, "persisted latency test failure", errMsg, &topicCfg.AlertPolicy)
		AnalyticsLatencyReport(clusterName, testName, err.Error(), -1, false, false)
	} else if !result.InOrderDelivery {
		errMsg := fmt.Sprintf("cluster %s, %s test Pulsar message received out of order", clusterName, testName)
		AnalyticsLatencyReport(clusterName, testName, "message delivery out of order", int(result.Latency.Milliseconds()), false, true)
		VerboseAlert(clusterName+"-latency-outoforder", errMsg, 3*time.Minute)
	} else if result.Latency > expectedLatency {
		stdVerdict.Add(float64(result.Latency.Microseconds()))
		errMsg := fmt.Sprintf("cluster %s, %s test message latency %v over the budget %v",
			clusterName, testName, result.Latency, expectedLatency)
		AnalyticsLatencyReport(clusterName, testName, "", int(result.Latency.Milliseconds()), true, false)
		VerboseAlert(clusterName+"-latency", errMsg, 3*time.Minute)
		ReportIncident(clusterName, clusterName, "persisted latency test failure", errMsg, &topicCfg.AlertPolicy)
	} else if stddev, mean, within6Sigma := stdVerdict.Push(float64(result.Latency.Microseconds())); !within6Sigma && stddev > 0 && mean > 0 {
		errMsg := fmt.Sprintf("cluster %s, %s test message latency %v μs over six standard deviation %v μs and mean is %v μs",
			clusterName, testName, result.Latency.Microseconds(), stddev, mean)
		// 5 ms = 5,000 μs
		if mean > 5000 {
			errMsg = fmt.Sprintf("cluster %s, %s test message latency %v over six standard deviation %v ms and mean is %v ms",
				clusterName, testName, result.Latency, float64(stddev/1000.0), float64(mean/1000.0))
		}
		AnalyticsLatencyReport(clusterName, testName, "", int(result.Latency.Milliseconds()), true, false)
		VerboseAlert(clusterName+"-latency-stddev", errMsg, 10*time.Minute)
		// standard deviation does not generate alerts
		// ReportIncident(clusterName, clusterName, "persisted latency test failure", errMsg, &topicCfg.AlertPolicy)
	} else {
		log.Infof("succeeded to sent %d messages to topic %s on %s test cluster %s",
			len(payloads), topicCfg.TopicName, testName, topicCfg.PulsarURL)
		AnalyticsLatencyReport(clusterName, testName, "", int(result.Latency.Milliseconds()), true, true)
		ClearIncident(clusterName)
	}
	if result.Latency < failedLatency {
		PromLatencySum(GetGaugeType(topicCfg.Name), clusterName, result.Latency)
	}
}

func expectedMessage(payload, expected string) string {
	if strings.HasPrefix(expected, "$") {
		return fmt.Sprintf("%s%s", payload, expected[1:len(expected)])
	}
	return payload
}

func testPartitionTopic(clusterName, token string, cfg TopicCfg) {
	trustStore := util.AssignString(cfg.TrustStore, GetConfig().TrustStore, "/etc/ssl/certs/ca-bundle.crt")
	testName := "partition-topics-test"
	component := clusterName + "-" + testName
	pt, err := getPartition(cfg, token, trustStore)
	if err != nil {
		errMsg := fmt.Sprintf("%s failed to create PartitionTopic test object, error: %v", component, err)
		ReportIncident(component, component, "persisted failure to create partition topic test client", errMsg, &cfg.AlertPolicy)
		return
	}
	pulsarClient, err := GetPulsarClient(cfg.PulsarURL, token)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s failed create Pulsar Client with error: %v", component, testName, err)
		Alert(errMsg)
		ReportIncident(component, component, "partition topic test failure", errMsg, &cfg.AlertPolicy)
		return
	}

	latency, err := pt.TestPartitionTopic(pulsarClient)
	if err != nil {
		errMsg := fmt.Sprintf("cluster %s, %s partition topic test failed with Pulsar error: %v", component, testName, err)
		Alert(errMsg)
		ReportIncident(component, component, "partition topic test failure", errMsg, &cfg.AlertPolicy)
		return
	}
	expectedLatency := util.TimeDuration(cfg.LatencyBudgetMs, latencyBudget, time.Millisecond)
	if latency > expectedLatency || latency == 0 {
		errMsg := fmt.Sprintf("cluster %s, partition topic test message latency %v over the budget %v",
			component, latency, expectedLatency)
		Alert(errMsg)
		ReportIncident(component, component, "partition topic test has over budget latency", errMsg, &cfg.AlertPolicy)
	} else {
		log.Infof("%d partition topics test successfully passed with latency %v", pt.NumberOfPartitions, latency)
		ClearIncident(component)
	}
}

func getPartition(cfg TopicCfg, token, trustStore string) (*topic.PartitionTopics, error) {
	pt, ok := partitionTopics[cfg.TopicName]
	if !ok {
		var err error
		pt, err = topic.NewPartitionTopic(cfg.PulsarURL, token, trustStore, cfg.TopicName, cfg.AdminURL, cfg.NumberOfPartitions)
		if err != nil {
			return nil, err
		}
		partitionTopics[cfg.TopicName] = pt
	}

	return pt, pt.VerifyPartitionTopic()
}
