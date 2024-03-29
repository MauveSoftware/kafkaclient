package kafkaclient

// Message is a kafka message
type Message struct {
	// Topic specifies the name of the topic
	Topic string

	// Partition the message was stored in
	Partition int32

	// Payload is the payload of the message
	Payload []byte

	// Offset is the offset of the message
	Offset int64

	// Key is the message key
	Key []byte
}
