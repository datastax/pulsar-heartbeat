package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	latencyBudget = 2400 // in Millisecond integer, will convert to time.Duration in evaluation
	failedLatency = 100 * time.Second
)

var (
	clients = make(map[string]pulsar.Client)
)

// PubSubLatency the latency including successful produce and consume of a message
func PubSubLatency(tokenStr, uri, topicName string) (time.Duration, error) {
	// uri is in the form of pulsar+ssl://useast1.gcp.kafkaesque.io:6651
	client, ok := clients[uri]
	if !ok {

		// Configuration variables pertaining to this consumer
		// RHEL CentOS:
		trustStore := AssignString(GetConfig().PulsarPerfConfig.TrustStore, "/etc/ssl/certs/ca-bundle.crt")
		// Debian Ubuntu:
		// trustStore := '/etc/ssl/certs/ca-certificates.crt'
		// OSX:
		// Export the default certificates to a file, then use that file:
		// security find-certificate -a -p /System/Library/Keychains/SystemCACertificates.keychain > ./ca-certificates.crt
		// trust_certs='./ca-certificates.crt'

		token := pulsar.NewAuthenticationToken(tokenStr)

		var err error
		client, err = pulsar.NewClient(pulsar.ClientOptions{
			URL:                   uri,
			Authentication:        token,
			TLSTrustCertsFilePath: trustStore,
		})

		if err != nil {
			return failedLatency, err
		}
		clients[uri] = client
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
		return failedLatency, err
	}

	defer producer.Close()

	subscriptionName := "latency-measure"
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})

	if err != nil {
		defer client.Close() //must defer to allow producer to be closed first
		delete(clients, uri)
		return failedLatency, err
	}
	defer consumer.Close()

	// the original sent time to notify the receiver for latency calculation
	timeCounter := make(chan time.Time, 1)

	// notify the main thread with the latency to complete the exit
	completeChan := make(chan time.Duration, 1)

	// error report channel
	errorChan := make(chan error, 1)

	payloadStr := "measure-latency123" + time.Now().Format(time.UnixDate)

	go func() {
		cCtx := context.Background()
		loop := true

		for loop {
			msg, err := consumer.Receive(cCtx)
			if err != nil {
				loop = false // play safe?
				errorChan <- fmt.Errorf("consumer Receive() error: %v", err)
				break
			}
			receivedStr := string(msg.Payload())
			if payloadStr == receivedStr {
				loop = false
				select {
				case sentTime := <-timeCounter:
					completeChan <- time.Now().Sub(sentTime)

				case <-time.Tick(5 * time.Second):
					// this is impossible case that producer must have sent signal
					errMsg := fmt.Sprintf("consumer received message, but timed out on producer report time")
					errorChan <- errors.New(errMsg)
				}
			}
			consumer.Ack(msg)
			log.Println("consumer received ", receivedStr)
		}

	}()

	ctx := context.Background()

	// Create a different message to send asynchronously
	asyncMsg := pulsar.ProducerMessage{
		Payload: []byte(payloadStr),
	}

	sentTime := time.Now()
	// Attempt to send the message asynchronously and handle the response
	producer.SendAsync(ctx, &asyncMsg, func(messageId pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
		if err != nil {
			errMsg := fmt.Sprintf("fail to instantiate Pulsar client: %v", err)
			log.Println(errMsg)
			// report error and exit
			errorChan <- errors.New(errMsg)
		}
		timeCounter <- sentTime

		log.Println("successfully published ", string(msg.Payload), sentTime)
	})

	select {
	case receiverLatency := <-completeChan:
		return receiverLatency, nil
	case reportedErr := <-errorChan:
		return failedLatency, reportedErr
	case <-time.Tick(15 * time.Second):
		return failedLatency, errors.New("latency measure not received after timeout")
	}
}

// MeasureLatency measures pub sub latency of each cluster
func MeasureLatency() {
	token := AssignString(GetConfig().PulsarPerfConfig.Token, GetConfig().PulsarOpsConfig.MasterToken)
	for _, cluster := range GetConfig().PulsarPerfConfig.TopicCfgs {
		expectedLatency := TimeDuration(cluster.LatencyBudgetMs, latencyBudget, time.Millisecond)
		log.Printf("test cluster %s on topic %s with latency budget %v\n", cluster.PulsarURL, cluster.TopicName, expectedLatency)
		latency, err := PubSubLatency(token, cluster.PulsarURL, cluster.TopicName)

		// uri is in the form of pulsar+ssl://useast1.gcp.kafkaesque.io:6651
		clusterName := getNames(cluster.PulsarURL)
		log.Printf("cluster %s has message latency %v", clusterName, latency)
		if err != nil {
			errMsg := fmt.Sprintf("cluster %s latency test Pulsar error: %v", clusterName, err)
			Alert(errMsg)
			ReportIncident(clusterName, "persisted latency test failure", errMsg, &cluster.AlertPolicy)
		} else if latency > expectedLatency {
			errMsg := fmt.Sprintf("cluster %s message latency %v over the budget %v",
				clusterName, latency, expectedLatency)
			Alert(errMsg)
			ReportIncident(clusterName, "persisted latency test failure", errMsg, &cluster.AlertPolicy)
		} else {
			ClearIncident(clusterName)
		}
		PromLatencySum(MsgLatencyGaugeOpt(), clusterName, latency)
	}
}

// getNames in the format for reporting and Prometheus metrics
// Input URL pulsar+ssl://useast1.gcp.kafkaesque.io:6651
func getNames(url string) string {
	name := strings.Split(Trim(url), ":")[1]
	clusterName := strings.Replace(name, "//", "", -1)
	return clusterName
}
