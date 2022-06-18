package internal

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type WrongTypeError struct {
	error
}

func NewWrongTypeError(desiredType string) WrongTypeError {
	return WrongTypeError{errors.Errorf("expected to get '%s', but not found one", desiredType)}
}

func (err WrongTypeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type LazyCache[K comparable, V any] struct {
	cache      map[K]V
	load       func(key K) (value V, err error)
	cacheMutex sync.Mutex
}

func NewLazyCache[K comparable, V any](load func(key K) (value V, err error)) *LazyCache[K, V] {
	return &LazyCache[K, V]{
		make(map[K]V),
		load,
		sync.Mutex{},
	}
}

func (lazyCache *LazyCache[K, V]) Load(key K) (value V, exists bool, err error) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	if value, ok := lazyCache.cache[key]; ok {
		return value, true, nil
	}
	value, err = lazyCache.load(key)
	if err == nil {
		lazyCache.cache[key] = value
	}
	return value, false, err
}

func (lazyCache *LazyCache[K, V]) LoadExisting(key K) (value V, exists bool) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	value, exists = lazyCache.cache[key]
	return
}

func (lazyCache *LazyCache[K, V]) Store(key K, value V) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	lazyCache.cache[key] = value
}

// Range calls reduce sequentially for each key and value present in the cache.
// If reduce returns false, range stops the iteration.
func (lazyCache *LazyCache[K, V]) Range(reduce func(key K, value V) bool) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	for key, value := range lazyCache.cache {
		if !reduce(key, value) {
			break
		}
	}
}
