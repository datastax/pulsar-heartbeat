package util

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// ConsumerResult is Pulsar Consumer result for channel communication
type ConsumerResult struct {
	Err             error
	InOrderDelivery bool
	Latency         time.Duration
	Timestamp       time.Time
}

// GetPulsarClient gets the pulsar client object
// Note: the caller has to Close() the client object
func GetPulsarClient(pulsarURL, tokenStr, trustStore string) (pulsar.Client, error) {
	clientOpt := pulsar.ClientOptions{
		URL:               pulsarURL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	}

	if tokenStr != "" {
		clientOpt.Authentication = pulsar.NewAuthenticationToken(tokenStr)
	}

	if strings.HasPrefix(pulsarURL, "pulsar+ssl://") {
		// trustStore := util.AssignString(GetConfig().TrustStore, "/etc/ssl/certs/ca-bundle.crt")
		if trustStore == "" {
			return nil, fmt.Errorf("fatal error: missing trustStore while pulsar+ssl tls is enabled")
		}
		clientOpt.TLSTrustCertsFilePath = trustStore
	}

	return pulsar.NewClient(clientOpt)
}

// VerifyMessageByPulsarConsumer instantiates a Pulsar consumer and verifies an expected message
func VerifyMessageByPulsarConsumer(client pulsar.Client, topicName, expectedMessage string, completeChan chan *ConsumerResult) error {
	topicParts := strings.Split(topicName, "/")
	subscriptionName := "partition-sub" + topicParts[len(topicParts)-1]
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	if err != nil {
		log.Printf("failed to created partition topic consumer, error: %v", err)
		return err
	}
	defer consumer.Close()

	receivedCount := 0
	receiveTimeout := 30 * time.Second
	start := time.Now()
	for time.Since(start) < time.Minute {
		cCtx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
		defer cancel()

		log.Printf("%s wait to receive on message count %d", topicName, receivedCount)
		receivedCount++
		msg, err := consumer.Receive(cCtx)
		if err != nil {
			completeChan <- &ConsumerResult{
				Err: fmt.Errorf("consumer Receive() error: %v", err),
			}
			break
		}
		consumer.Ack(msg)
		// log.Printf("received message %s and expected message %s", string(msg.Payload()), expectedMessage)
		if expectedMessage == string(msg.Payload()) {
			log.Printf("expected message received by %s", topicName)
			completeChan <- &ConsumerResult{
				InOrderDelivery: true,
				Timestamp:       time.Now(),
			}
			return nil
		}
	}
	return nil
}
