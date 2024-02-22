package kafkaclient

import "github.com/IBM/sarama"

// Emiter does emit messages to kafka
type Emiter interface {
	EmitMessage(b []byte) error
}

type emiter struct {
	topic    string
	cl       *Client
	producer sarama.SyncProducer
}

// Emit message to kafka
func (me *emiter) EmitMessage(b []byte) error {
	_, _, err := me.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     me.topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(b),
	})

	return err
}
