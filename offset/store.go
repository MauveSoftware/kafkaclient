package offset

// Store manages offsets
type Store interface {
	// Next returns the next offset for a specific topic partition
	Next(topic string, partition int32) (int64, error)

	// Update sets a new offset for a specific topic partition
	Update(topic string, partition int32, value int64)
}
