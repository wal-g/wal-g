package memory

import (
	"bytes"
	"slices"
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
	// keep track of insertion/update order to resolve races where two files are created in the same instant
	order   *[]string
	timeNow func() time.Time
}

func NewKVS(opts ...func(*KVS)) *KVS {
	s := &KVS{underlying: &sync.Map{}, timeNow: time.Now, order: &[]string{}}
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
	popOrderedKey(key, storage)
	*storage.order = append(*storage.order, key)
	storage.underlying.Store(key, TimeStampData(value, storage.timeNow))
}

func popOrderedKey(key string, storage *KVS) {
	// always pop the current key from order
	idx := slices.IndexFunc(*storage.order, func(c string) bool { return c == key })
	if idx != -1 {
		*storage.order = append((*storage.order)[:idx], (*storage.order)[idx+1:]...)
	}
}

func (storage *KVS) Delete(key string) {
	popOrderedKey(key, storage)
	storage.underlying.Delete(key)
}

func (storage *KVS) Range(callback func(key string, value TimeStampedData) bool) {
	// keep ordering consistent with last updated/insertion per https://go.dev/blog/maps#iteration-order
	for _, v := range *storage.order {
		data, _ := storage.Load(v)
		callback(v, data)
	}
}
