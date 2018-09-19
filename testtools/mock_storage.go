package testtools

import (
	"bytes"
	"sync"
)

type MockStorage struct {
	underlying *sync.Map
}

func NewMockStorage() *MockStorage {
	return &MockStorage{&sync.Map{}}
}

func (storage *MockStorage) Load(key string) (value bytes.Buffer, exists bool) {
	valueInterface, ok := storage.underlying.Load(key)
	if !ok {
		return bytes.Buffer{}, ok
	}
	return valueInterface.(bytes.Buffer), ok
}

func (storage *MockStorage) Store(key string, value bytes.Buffer) {
	storage.underlying.Store(key, value)
}

func (storage *MockStorage) IsEmpty() bool {
	isEmpty := true
	storage.underlying.Range(func(key, value interface{}) bool {
		isEmpty = false
		return true
	})
	return isEmpty
}
