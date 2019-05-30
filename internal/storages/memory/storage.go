package memory

import (
	"bytes"
	"github.com/wal-g/wal-g/utility"
	"sync"
	"time"
)

type TimeStampedData struct {
	Data      bytes.Buffer
	Timestamp time.Time
}

func TimeStampData(data bytes.Buffer) TimeStampedData {
	return TimeStampedData{data, utility.TimeNowCrossPlatformLocal()}
}

// Storage is supposed to be used for tests. It doesn't guarantee data safety!
type Storage struct {
	underlying *sync.Map
}

func NewStorage() *Storage {
	return &Storage{&sync.Map{}}
}

func (storage *Storage) Load(key string) (value TimeStampedData, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return TimeStampedData{}, ok
	}
	return valueInterface.(TimeStampedData), ok
}

func (storage *Storage) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, TimeStampData(value))
}

func (storage *Storage) Delete(key string) {
	storage.underlying.Delete(key)
}

func (storage *Storage) Range(callback func(key string, value TimeStampedData) bool) {
	storage.underlying.Range(func(iKey, iValue interface{}) bool {
		return callback(iKey.(string), iValue.(TimeStampedData))
	})
}
