package kafkaclient

import (
	"github.com/IBM/sarama"
)

type topicConsumer struct {
	client   *Client
	topic    string
	consumer sarama.PartitionConsumer
	done     chan struct{}
}

func (tc *topicConsumer) consume() {
	for {
		select {
		case msg := <-tc.consumer.Messages():
			tc.client.messages <- &Message{
				Topic:   tc.topic,
				Payload: msg.Value,
				Offset:  msg.Offset,
				Key:     msg.Key,
			}
		case err := <-tc.consumer.Errors():
			tc.client.errors <- err
		case <-tc.done:
			return
		}
	}
}

func (tc *topicConsumer) close() {
	tc.done <- struct{}{}
}
