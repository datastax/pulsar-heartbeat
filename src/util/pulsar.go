package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apex/log"
)

// ConsumerResult is Pulsar Consumer result for channel communication
type ConsumerResult struct {
	Err             error
	InOrderDelivery bool
	Latency         time.Duration
	Timestamp       time.Time
}

// VerifyMessageByPulsarConsumer instantiates a Pulsar consumer and verifies an expected message
func VerifyMessageByPulsarConsumer(client pulsar.Client, topicName, expectedMessage string, completeChan chan *ConsumerResult) error {
	topicParts := strings.Split(topicName, "/")
	subscriptionName := "partition-sub" + topicParts[len(topicParts)-1]
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		ReceiverQueueSize:           1,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		log.Errorf("failed to created partition topic consumer, error: %v", err)
		return err
	}
	defer consumer.Close()

	receivedCount := 0
	receiveTimeout := 60 * time.Second
	start := time.Now()
	for time.Since(start) < 90*time.Second { // TODO: this should be passed in as timeout parameter
		cCtx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
		defer cancel()

		log.Infof("%s wait to receive on message count %d", topicName, receivedCount)
		receivedCount++
		msg, err := consumer.Receive(cCtx)
		if err != nil {
			completeChan <- &ConsumerResult{
				Err: fmt.Errorf("consumer Receive() error: %v", err),
			}
			break
		}
		consumer.Ack(msg)
		if expectedMessage == string(msg.Payload()) {
			log.Infof("expected message received by %s", topicName)
			completeChan <- &ConsumerResult{
				InOrderDelivery: true,
				Timestamp:       time.Now(),
			}
			return nil
		}
	}
	return nil
}
