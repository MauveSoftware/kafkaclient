package database

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"gorm.io/gorm"

	"github.com/MauveSoftware/kafkaclient"
	"github.com/MauveSoftware/kafkaclient/offset"
)

func NewStore(db *gorm.DB, persistInterval time.Duration) offset.Store {
	db.AutoMigrate(&Offset{})

	o := &databaseStore{
		db:     db,
		values: make(map[key]*Offset),
	}

	go func() {
		for {
			<-time.After(persistInterval)
			o.persist()
		}
	}()

	return o
}

type key struct {
	topic     string
	partition int32
}

type databaseStore struct {
	db     *gorm.DB
	values map[key]*Offset
}

func (o *databaseStore) Next(topic string, partition int32) (int64, error) {
	offset := Offset{}
	err := o.db.Where(&Offset{
		Topic:     topic,
		Partition: partition,
	}).FirstOrCreate(&offset).Error
	if err != nil {
		return 0, fmt.Errorf("could not get offset for topic %s", topic, err)
	}

	k := key{
		topic:     topic,
		partition: partition,
	}
	o.values[k] = &offset

	if offset.Offset == 0 {
		return sarama.OffsetOldest, nil
	}

	return offset.Offset + 1, nil
}

func (o *databaseStore) Update(topic string, partition int32, value int64) {
	k := key{
		topic:     topic,
		partition: partition,
	}
	v := o.values[k]
	v.Offset = value
}

func (o *databaseStore) persist() {
	kafkaclient.StandardLogger().Debugf("Persisting offsets")
	for _, v := range o.values {
		err := o.db.Save(v).Error
		if err != nil {
			kafkaclient.StandardLogger().Errorf("could not persist offset for topic %s: %v", v.Topic, err)
		}
	}
}
