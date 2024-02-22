package kafkaclient

import "github.com/IBM/sarama"

type Emiter interface {
	EmitMessage(b []byte) error
}

type emiter struct {
	topic    string
	cl       *Client
	producer sarama.SyncProducer
}

func (me *emiter) EmitMessage(b []byte) error {
	_, _, err := me.producer.SendMessage(&sarama.ProducerMessage{
		Topic:     me.topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(b),
	})

	return err
}
