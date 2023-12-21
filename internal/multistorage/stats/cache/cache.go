package cache

import (
	"fmt"
	"time"

	"github.com/wal-g/tracelog"
)

type Cache interface {
	// Read the cached state for storages with specified names and split them by relevance according to the cache TTL.
	Read(storageNames ...string) (relevant, outdated AliveMap, err error)

	// ApplyExplicitCheckResult with specifying which checked storages were alive, and return the new cached state for
	// all storages with specified names.
	ApplyExplicitCheckResult(checkResult AliveMap, checkTime time.Time, storageNames ...string) (AliveMap, error)

	// ApplyOperationResult to the cache for a specific storage, indicating whether the storage was alive, and the
	// weight of the performed operation.
	ApplyOperationResult(storage string, alive bool, weight float64)

	// Flush changes made in memory to the shared cache file.
	Flush()
}

var _ Cache = &cache{}

type cache struct {
	// usedKeys matches all storage names that can be requested from this cache with corresponding keys.
	usedKeys map[string]Key
	ttl      time.Duration

	// shMem keeps the intra-process cache that's shared among different threads and subsequent storages requests.
	shMem *SharedMemory

	// shFile is the path to the file that keeps the inter-process cache that's shared between different command executions.
	shFile             *SharedFile
	shFileUsed         bool
	shFileFlushTimeout time.Duration

	emaParams *EMAParams
}

type Option func(c *cache)

func WithCustomFlushTimeout(timeout time.Duration) Option {
	return func(c *cache) {
		c.shFileFlushTimeout = timeout
	}
}

const defaultFlushTimeout = 5 * time.Minute

type Config struct {
	TTL       time.Duration
	EMAParams *EMAParams
}

func New(
	usedKeys map[string]Key,
	config *Config,
	sharedMem *SharedMemory,
	sharedFile *SharedFile,
	opts ...Option,
) (Cache, error) {
	c := &cache{
		usedKeys:           usedKeys,
		ttl:                config.TTL,
		shMem:              sharedMem,
		shFile:             sharedFile,
		shFileUsed:         sharedFile != nil,
		shFileFlushTimeout: defaultFlushTimeout,
		emaParams:          config.EMAParams,
	}
	for _, o := range opts {
		o(c)
	}
	err := c.emaParams.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid EMA params: %w", err)
	}
	return c, nil
}

func (c *cache) Read(storageNames ...string) (relevant, outdated AliveMap, err error) {
	c.shMem.Lock()
	defer c.shMem.Unlock()

	storageKeys, err := c.correspondingKeys(storageNames...)
	if err != nil {
		return nil, nil, err
	}

	allMemRelevant := c.shMem.Statuses.isRelevant(c.ttl, storageKeys...)
	if allMemRelevant {
		return c.shMem.Statuses.filter(storageKeys).aliveMap(c.emaParams), nil, nil
	}

	var fileStatuses storageStatuses
	if c.shFileUsed {
		fileStatuses, err = c.shFile.read()
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read storage status cache file %q: %v", c.shFile.Path, err)
		}
	}
	memAndFileMerged := mergeByRelevance(c.shMem.Statuses, fileStatuses)

	c.shMem.Statuses = memAndFileMerged

	relevantStatuses, outdatedStatuses := memAndFileMerged.splitByRelevance(c.ttl, storageKeys)
	relevantAliveMap := relevantStatuses.filter(storageKeys).aliveMap(c.emaParams)
	outdatedAliveMap := outdatedStatuses.filter(storageKeys).aliveMap(c.emaParams)
	return relevantAliveMap, outdatedAliveMap, nil
}

func (c *cache) ApplyExplicitCheckResult(checkResult AliveMap, checkTime time.Time, storageNames ...string) (AliveMap, error) {
	c.shMem.Lock()
	defer c.shMem.Unlock()

	checkResultByKeys := make(map[Key]bool, len(checkResult))
	for _, key := range c.usedKeys {
		if res, ok := checkResult[key.Name]; ok {
			checkResultByKeys[key] = res
		}
	}

	c.shMem.Statuses = c.shMem.Statuses.applyExplicitCheckResult(checkResultByKeys, checkTime)

	storageKeys, err := c.correspondingKeys(storageNames...)
	if err != nil {
		return nil, err
	}
	aliveMap := c.shMem.Statuses.filter(storageKeys).aliveMap(c.emaParams)

	if !c.shFileUsed {
		return aliveMap, nil
	}
	shFileRelevant := time.Since(c.shFile.Updated) < c.shFileFlushTimeout
	if shFileRelevant {
		return aliveMap, nil
	}
	c.flushFileFromMem()
	return aliveMap, nil
}

func (c *cache) correspondingKeys(names ...string) ([]Key, error) {
	keys := make([]Key, 0, len(names))
	for _, name := range names {
		key, ok := c.usedKeys[name]
		if !ok {
			return nil, fmt.Errorf("unknown storage %q", name)
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func (c *cache) ApplyOperationResult(storage string, alive bool, weight float64) {
	c.shMem.Lock()
	defer c.shMem.Unlock()

	var key Key
	keyFound := false
	for _, k := range c.usedKeys {
		if k.Name == storage {
			key = k
			keyFound = true
			break
		}
	}
	if !keyFound {
		return
	}

	c.shMem.Statuses[key] = c.shMem.Statuses[key].applyOperationResult(c.emaParams, alive, weight, time.Now())

	if !c.shFileUsed {
		return
	}
	shFileRelevant := time.Since(c.shFile.Updated) < c.shFileFlushTimeout
	if shFileRelevant {
		return
	}
	c.flushFileFromMem()
}

func (c *cache) Flush() {
	if !c.shFileUsed {
		return
	}

	c.shMem.Lock()
	defer c.shMem.Unlock()

	c.flushFileFromMem()
}

func (c *cache) flushFileFromMem() {
	fileStatuses, err := c.shFile.read()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read storage status cache file to merge it with memory %q: %v", c.shFile.Path, err)
	}
	memAndFileMerged := mergeByRelevance(c.shMem.Statuses, fileStatuses)
	err = c.shFile.write(memAndFileMerged)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to flush storage status cache file: %v", err)
	}
}
