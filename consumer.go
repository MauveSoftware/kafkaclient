package kafkaclient

import (
	"github.com/IBM/sarama"
	"github.com/MauveSoftware/kafkaclient/offset"
)

type topicConsumer struct {
	client    *client
	topic     string
	partition int32
	offsets   offset.Store
	consumer  sarama.PartitionConsumer
	done      chan struct{}
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
			tc.offsets.Update(tc.topic, tc.partition, msg.Offset)
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
