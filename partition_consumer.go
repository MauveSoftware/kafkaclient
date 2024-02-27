package kafkaclient

import (
	"github.com/IBM/sarama"
	"github.com/MauveSoftware/kafkaclient/offset"
)

type partitionConsumer struct {
	topic         string
	partition     int32
	offsets       offset.Store
	sPartConsumer sarama.PartitionConsumer
	messages      chan<- *Message
	errors        chan<- *sarama.ConsumerError
	done          chan struct{}
}

func (pc *partitionConsumer) consume() {
	for {
		select {
		case msg := <-pc.sPartConsumer.Messages():
			pc.messages <- &Message{
				Topic:     pc.topic,
				Partition: pc.partition,
				Payload:   msg.Value,
				Offset:    msg.Offset,
				Key:       msg.Key,
			}
			pc.offsets.Update(pc.topic, pc.partition, msg.Offset)
		case err := <-pc.sPartConsumer.Errors():
			pc.errors <- err
		case <-pc.done:
			return
		}
	}
}

func (pc *partitionConsumer) close() {
	pc.done <- struct{}{}
}
