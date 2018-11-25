package testtools

import (
	"bytes"
	"sync"
)

type InMemoryStorage struct {
	underlying *sync.Map
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{&sync.Map{}}
}

func (storage *InMemoryStorage) Load(key string) (value bytes.Buffer, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return bytes.Buffer{}, ok
	}
	return valueInterface.(bytes.Buffer), ok
}

func (storage *InMemoryStorage) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, value)
}
