package memory

import (
	"bytes"
	"sync"
	"time"
)

// This function is needed for being cross-platform
func CeilTimeUpToMicroseconds(timeToCeil time.Time) time.Time {
	if timeToCeil.Nanosecond()%1000 != 0 {
		timeToCeil = timeToCeil.Add(time.Microsecond)
		timeToCeil = timeToCeil.Add(-time.Duration(timeToCeil.Nanosecond() % 1000))
	}
	return timeToCeil
}

type TimeStampedData struct {
	Data      bytes.Buffer
	Timestamp time.Time
	Size      int
}

func TimeStampData(data bytes.Buffer, timeNow func() time.Time) TimeStampedData {
	return TimeStampedData{data, CeilTimeUpToMicroseconds(timeNow()), data.Len()}
}

// KVS is supposed to be used for tests. It doesn't guarantee data safety!
// TODO: Get rid of the KVS and move this logic to the Folder.
type KVS struct {
	underlying *sync.Map
	timeNow    func() time.Time
}

func NewKVS(opts ...func(*KVS)) *KVS {
	s := &KVS{underlying: &sync.Map{}, timeNow: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
}

func WithCustomTime(timeNow func() time.Time) func(*KVS) {
	return func(s *KVS) {
		s.timeNow = timeNow
	}
}

func (storage *KVS) Load(key string) (value TimeStampedData, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return TimeStampedData{}, ok
	}
	return valueInterface.(TimeStampedData), ok
}

func (storage *KVS) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, TimeStampData(value, storage.timeNow))
}

func (storage *KVS) Delete(key string) {
	storage.underlying.Delete(key)
}

func (storage *KVS) Range(callback func(key string, value TimeStampedData) bool) {
	storage.underlying.Range(func(iKey, iValue interface{}) bool {
		return callback(iKey.(string), iValue.(TimeStampedData))
	})
}
