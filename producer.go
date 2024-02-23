package kafkaclient

import "github.com/IBM/sarama"

// Emiter does emit messages to kafka
type Emiter interface {
	// EmitMessage emits a message to a kafka topic
	EmitMessage(b []byte, partition int32) error
}

type emiter struct {
	topic    string
	cl       *client
	producer sarama.SyncProducer
}

// EmitMessage implements Emiter.EmitMessage
func (me *emiter) EmitMessage(b []byte, partition int32) error {
	_, _, err := me.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     me.topic,
		Partition: partition,
		Value:     sarama.ByteEncoder(b),
	})

	return err
}
