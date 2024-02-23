package database

type Offset struct {
	Topic     string `gorm:"primaryKey"`
	Partition int32  `gorm:"primaryKey"`
	Offset    int64
}
