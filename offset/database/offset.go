package database

type Offset struct {
	ID        uint
	Topic     string
	Partition int32
	Offset    int64
}
