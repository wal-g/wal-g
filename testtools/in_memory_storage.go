package testtools

import (
	"bytes"
	"sync"
	"time"
)

type TimeStampedData struct {
	Data      bytes.Buffer
	Timestamp time.Time
}

func TimeStampData(data bytes.Buffer) TimeStampedData {
	return TimeStampedData{data, time.Now()}
}

type InMemoryStorage struct {
	underlying *sync.Map
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{&sync.Map{}}
}

func (storage *InMemoryStorage) Load(key string) (value TimeStampedData, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return TimeStampedData{}, ok
	}
	return valueInterface.(TimeStampedData), ok
}

func (storage *InMemoryStorage) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, TimeStampData(value))
}

func (storage *InMemoryStorage) Range(callback func(key string, value TimeStampedData) bool) {
	storage.underlying.Range(func(iKey, iValue interface{}) bool {
		return callback(iKey.(string), iValue.(TimeStampedData))
	})
}
