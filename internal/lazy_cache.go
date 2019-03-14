package internal

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
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

type LazyCache struct {
	cache      map[interface{}]interface{}
	load       func(key interface{}) (value interface{}, err error)
	cacheMutex sync.Mutex
}

func NewLazyCache(load func(key interface{}) (value interface{}, err error)) *LazyCache {
	return &LazyCache{
		make(map[interface{}]interface{}),
		load,
		sync.Mutex{},
	}
}

func (lazyCache *LazyCache) Load(key interface{}) (value interface{}, exists bool, err error) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	if value, ok := lazyCache.cache[key]; ok {
		return value, true, nil
	}
	value, err = lazyCache.load(key)
	lazyCache.cache[key] = value
	return value, false, err
}

func (lazyCache *LazyCache) LoadExisting(key interface{}) (value interface{}, exists bool) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	value, exists = lazyCache.cache[key]
	return
}

func (lazyCache *LazyCache) Store(key, value interface{}) {
	lazyCache.cacheMutex.Lock()
	defer lazyCache.cacheMutex.Unlock()
	lazyCache.cache[key] = value
}

// Range calls reduce sequentially for each key and value present in the cache.
// If reduce returns false, range stops the iteration.
func (lazyCache *LazyCache) Range(reduce func(key, value interface{}) bool) {
	for key, value := range lazyCache.cache {
		if !reduce(key, value) {
			break
		}
	}
}
