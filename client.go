package kafkaclient

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/MauveSoftware/kafkaclient/offset"
)

// TopicFilter determines if a topic should be included in the result
type TopicFilter func(string) bool

// Client interacts with the kafka cluster
type Client interface {
	// Topics gets a list of topics from the kafka cluster
	Topics(filter TopicFilter) ([]string, error)

	// Partitions returns the partitions for a topic
	Partitions(topic string) ([]int32, error)

	// Close disconnects from the kafka cluster
	Close()

	// Creates an consumer
	CreateConsumer() Consumer

	// CreateEmiter creates an emiter to send messages to topic
	CreateEmiter(topic string) (Emiter, error)
}

type client struct {
	sClient   sarama.Client
	sConsumer sarama.Consumer
	offsets   offset.Store
	consumers []*consumer
}

// NewClient returns a new client connected to the specified kafka cluster
func NewClient(address string, offsets offset.Store) (Client, error) {
	cfg := newConfig()

	logger.Infof("Connecting to kafka on %s", address)
	c, err := sarama.NewClient([]string{address}, cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to kafka: %w", err)
	}

	csmr, err := sarama.NewConsumerFromClient(c)
	if err != nil {
		return nil, fmt.Errorf("could create consumer: %w", err)
	}

	return &client{
		sClient:   c,
		sConsumer: csmr,
		offsets:   offsets,
		consumers: make([]*consumer, 0),
	}, nil
}

func newConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	cfg.Consumer.Group.Session.Timeout = 5 * time.Minute

	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	return cfg
}

// Topics implements Client.Topics
func (cl *client) Topics(filter TopicFilter) ([]string, error) {
	topics, err := cl.sConsumer.Topics()
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

// Partitions implements Client.Partitions
func (cl *client) Partitions(topic string) ([]int32, error) {
	return cl.sClient.Partitions(topic)
}

// Creates an consumer
func (cl *client) CreateConsumer() Consumer {
	con := newConsumer(cl.sConsumer, cl.offsets)
	cl.consumers = append(cl.consumers, con)

	return con
}

// CreateEmiter implements Client.CreateEmiter
func (cl *client) CreateEmiter(topic string) (Emiter, error) {
	prod, err := sarama.NewSyncProducerFromClient(cl.sClient)
	if err != nil {
		return nil, fmt.Errorf("could not create producer: %w", err)
	}

	return &emiter{
		topic:    topic,
		producer: prod,
	}, nil
}

// Close implements Client.Close
func (cl *client) Close() {
	for _, con := range cl.consumers {
		con.close()
	}

	cl.sConsumer.Close()
}
