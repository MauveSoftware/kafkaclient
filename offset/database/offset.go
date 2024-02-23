package database

type Offset struct {
	ID        uint64
	Topic     string
	Partition int32
	Offset    int64
}
