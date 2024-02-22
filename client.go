package kafkaclient

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// NewClient returns a new client connected to the specified kafka cluster
func NewClient(address string) (*Client, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	cfg.Consumer.Group.Session.Timeout = 5 * time.Minute
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	logrus.Infof("Connecting to kafka on %s", address)
	c, err := sarama.NewClient([]string{address}, cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to kafka: %w", err)
	}

	con, err := sarama.NewConsumerFromClient(c)
	if err != nil {
		return nil, fmt.Errorf("could create consumer: %w", err)
	}

	return &Client{
		client:         c,
		consumer:       con,
		messages:       make(chan *Message),
		errors:         make(chan *sarama.ConsumerError),
		topicConsumers: make([]*topicConsumer, 0),
	}, nil
}

// Client interacts with the kafka cluster
type Client struct {
	client         sarama.Client
	consumer       sarama.Consumer
	messages       chan *Message
	errors         chan *sarama.ConsumerError
	topicConsumers []*topicConsumer
}

// Topics gets a list of topics from the kafka cluster
func (cl *Client) Topics(filter func(string) bool) ([]string, error) {
	topics, err := cl.consumer.Topics()
	if err != nil {
		return nil, fmt.Errorf("could not get topic list: %w", err)
	}

	filtered := make([]string, 0)
	for _, topic := range topics {
		if filter(topic) {
			filtered = append(filtered, topic)
		}
	}

	return filtered, nil
}

// ConsumeTopic starts consuming messages from the topic
func (cl *Client) ConsumeTopic(topic string, offset int64) error {
	logrus.Info("Consuming topic ", topic)
	c, err := cl.consumer.ConsumePartition(topic, 0, offset)
	if err != nil {
		if err == sarama.ErrOffsetOutOfRange && offset != sarama.OffsetNewest {
			logrus.Warnf("could not consume topic %s with offset %d (out of range). trying to get newest messages.", topic, offset)
			return cl.ConsumeTopic(topic, sarama.OffsetNewest)
		}

		return fmt.Errorf("could not consume topic %s: %w", topic, err)
	}

	tc := &topicConsumer{
		consumer: c,
		client:   cl,
		topic:    topic,
		done:     make(chan struct{}),
	}

	go tc.consume()

	cl.topicConsumers = append(cl.topicConsumers, tc)

	return nil
}

// Close disconnects from the kafka cluster
func (cl *Client) Close() {
	for _, tc := range cl.topicConsumers {
		logrus.Infof("Closing kafka consumer for topic %s", tc.topic)
		tc.close()
	}

	cl.consumer.Close()

	close(cl.errors)
	close(cl.messages)
}

// Erorrs receives error messages
func (cl *Client) Errors() <-chan *sarama.ConsumerError {
	return cl.errors
}

// Messages receives messages consumed from topics
func (cl *Client) Messages() <-chan *Message {
	return cl.messages
}

// CreateEmiter creates an emiter to send messages to topic
func (cl *Client) CreateEmiter(topic string) (Emiter, error) {
	prod, err := sarama.NewSyncProducerFromClient(cl.client)
	if err != nil {
		return nil, fmt.Errorf("could create consumer: %w", err)
	}

	return &emiter{
		topic:    topic,
		cl:       cl,
		producer: prod,
	}, nil
}