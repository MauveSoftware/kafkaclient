package kafkaclient

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/MauveSoftware/kafkaclient/offset"
)

type Consumer interface {
	// ConsumeTopic starts consuming messages from the partition of the specified topic
	ConsumeTopicPartition(topic string, partition int32) error

	// Erorrs receives error messages
	Errors() <-chan *sarama.ConsumerError

	// Messages receives messages consumed from topics
	Messages() <-chan *Message
}

type consumer struct {
	messages           chan *Message
	errors             chan *sarama.ConsumerError
	offsets            offset.Store
	sConsumer          sarama.Consumer
	partitionConsumers []*partitionConsumer
}

func newConsumer(sConsumer sarama.Consumer, offsets offset.Store) *consumer {
	return &consumer{
		messages:  make(chan *Message),
		errors:    make(chan *sarama.ConsumerError),
		offsets:   offsets,
		sConsumer: sConsumer,
	}
}

// Erorrs implements Consumer.Errors
func (c *consumer) Errors() <-chan *sarama.ConsumerError {
	return c.errors
}

// Messages implements Consumer.Messages
func (c *consumer) Messages() <-chan *Message {
	return c.messages
}

// ConsumeTopicPartition implements Consumer.ConsumeTopicPartition
func (c *consumer) ConsumeTopicPartition(topic string, partition int32) error {
	logger.Infof("Consuming topic %s/%d", topic, partition)

	offset, err := c.offsets.Next(topic, partition)
	if err != nil {
		return fmt.Errorf("error while determining offset: %w", err)
	}

	return c.consume(topic, partition, offset)
}

func (c *consumer) consume(topic string, partition int32, offset int64) error {
	spc, err := c.sConsumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		if err == sarama.ErrOffsetOutOfRange && offset != sarama.OffsetNewest {
			logger.Warnf("could not consume topic %s with offset %d (out of range). trying to get newest messages.", topic, offset)
			return c.consume(topic, partition, sarama.OffsetNewest)
		}

		return fmt.Errorf("could not consume topic %s: %w", topic, err)
	}

	pc := &partitionConsumer{
		topic:         topic,
		partition:     partition,
		offsets:       c.offsets,
		sPartConsumer: spc,
		messages:      c.messages,
		errors:        c.errors,
		done:          make(chan struct{}),
	}

	go pc.consume()

	c.partitionConsumers = append(c.partitionConsumers, pc)

	return nil
}

func (c *consumer) close() {
	for _, pc := range c.partitionConsumers {
		logger.Infof("Closing kafka consumer for topic %s/%d", pc.topic, pc.partition)
		pc.close()
	}

	close(c.messages)
	close(c.errors)
}
