package cache

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
)

//go:generate mockgen -source status_cache.go -destination status_cache_mock.go -package cache
type StatusCache interface {
	AllAliveStorages() ([]string, error)
	FirstAliveStorage() (*string, error)
	SpecificStorage(name string) (bool, error)
}

type statusCache struct {
	storagesInOrder []Key
	ttl             time.Duration
	checker         AliveChecker

	// sharedMem keeps the intra-process cache that's shared between different threads and subsequent storages requests.
	sharedMem   *storageStatuses
	sharedMemMu *sync.Mutex

	// sharedFilePath is the path to the file that keeps the inter-process cache that's shared between different command executions.
	sharedFilePath string
	sharedFileUse  bool
}

type StatusCacheOpt func(c *statusCache)

func WithSharedMemory(mem *storageStatuses, memMu *sync.Mutex) StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedMem = mem
		c.sharedMemMu = memMu
	}
}

func WithSharedFile(filePath string) StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedFilePath = filePath
		c.sharedFileUse = true
	}
}

func WithoutSharedFile() StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedFileUse = false
	}
}

func NewStatusCache(
	storagesInOrder []Key,
	ttl time.Duration,
	checker AliveChecker,
	opts ...StatusCacheOpt,
) StatusCache {
	homeFileUse := true
	homeFilePath, err := HomeStatusFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Can't use storage status cache file from $HOME: %v", err)
		homeFileUse = false
	}

	c := &statusCache{
		storagesInOrder: storagesInOrder,
		ttl:             ttl,
		checker:         checker,

		sharedMem:      &globalMemCache,
		sharedMemMu:    globalMemCacheMu,
		sharedFilePath: homeFilePath,
		sharedFileUse:  homeFileUse,
	}

	for _, o := range opts {
		o(c)
	}

	return c
}

func (c *statusCache) AllAliveStorages() (allAlive []string, err error) {
	c.sharedMemMu.Lock()
	defer c.sharedMemMu.Unlock()

	if c.sharedMem.isRelevant(c.ttl, c.storagesInOrder...) {
		allAlive = c.sharedMem.getAllAlive(c.storagesInOrder)
		if len(allAlive) > 0 {
			return allAlive, nil
		}
	}

	var oldFile storageStatuses
	if c.sharedFileUse {
		oldFile, err = readFile(c.sharedFilePath)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	relevant, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)
	if len(outdated) == 0 {
		c.sharedMem = &oldFile
		allAlive = c.sharedMem.getAllAlive(c.storagesInOrder)
		if len(allAlive) > 0 {
			tracelog.InfoLogger.Printf("Take all alive storages from file cache: %v", allAlive)
			return allAlive, nil
		}
	}

	newFile := oldFile
	if len(outdated) > 0 {
		tracelog.InfoLogger.Printf(
			"Storage status cache is outdated, checking for alive again: %v",
			storageNames(outdated),
		)
		checkResult := c.checkForAlive(outdated...)
		newFile = updateFileContent(oldFile, checkResult)
		allAlive = newFile.getAllAlive(c.storagesInOrder)
	}

	defer func() {
		c.storeStatuses(newFile)
		tracelog.InfoLogger.Printf("Found alive storages: %v", allAlive)
	}()

	if len(allAlive) > 0 {
		return allAlive, nil
	}

	if len(relevant) > 0 {
		tracelog.InfoLogger.Printf(
			"All storages are dead in cache, rechecking relevant ones: %v",
			storageNames(relevant),
		)
		checkResult := c.checkForAlive(relevant...)
		newFile = updateFileContent(newFile, checkResult)
		allAlive = newFile.getAllAlive(c.storagesInOrder)
	}

	return allAlive, nil
}

func (c *statusCache) FirstAliveStorage() (firstAlive *string, err error) {
	c.sharedMemMu.Lock()
	defer c.sharedMemMu.Unlock()

	memFirstAlive, allRelevant := c.sharedMem.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if memFirstAlive != nil {
		return &memFirstAlive.Name, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There are no alive storages in memory cache")
	}

	var oldFile storageStatuses
	if c.sharedFileUse {
		oldFile, err = readFile(c.sharedFilePath)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	fileFirstAlive, allRelevant := oldFile.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if fileFirstAlive != nil {
		tracelog.InfoLogger.Printf("Take first alive storage from file cache: %s", fileFirstAlive.Name)
		(*c.sharedMem)[*fileFirstAlive] = oldFile[*fileFirstAlive]
		return &fileFirstAlive.Name, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There are no alive storages in file cache")
	}

	relevant, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)

	newFile := oldFile
	if len(outdated) > 0 {
		tracelog.InfoLogger.Printf("Storage status cache is outdated, checking for alive again: %v", storageNames(outdated))
		checkResult := c.checkForAlive(outdated...)
		newFile = updateFileContent(newFile, checkResult)
		firstAliveKey, _ := newFile.getRelevantFirstAlive(time.Duration(math.MaxInt64), c.storagesInOrder)
		if firstAliveKey != nil {
			firstAlive = &firstAliveKey.Name
		}
	}

	defer func() {
		c.storeStatuses(newFile)
		if firstAlive == nil {
			tracelog.InfoLogger.Print("Found no alive storages")
		} else {
			tracelog.InfoLogger.Printf("First found alive storage: %s", *firstAlive)
		}
	}()

	if firstAlive != nil {
		return firstAlive, nil
	}

	if len(relevant) > 0 {
		tracelog.InfoLogger.Printf("All storages are dead in cache, rechecking relevant ones: %v", storageNames(relevant))
		checkResult := c.checkForAlive(relevant...)
		newFile = updateFileContent(newFile, checkResult)
		firstAliveKey, _ := newFile.getRelevantFirstAlive(time.Duration(math.MaxInt64), c.storagesInOrder)
		if firstAliveKey != nil {
			firstAlive = &firstAliveKey.Name
		}
	}

	return firstAlive, nil
}

func (c *statusCache) SpecificStorage(name string) (alive bool, err error) {
	c.sharedMemMu.Lock()
	defer c.sharedMemMu.Unlock()

	var specific *Key
	for _, s := range c.storagesInOrder {
		if s.Name == name {
			cpy := s
			specific = &cpy
		}
	}
	if specific == nil {
		return false, fmt.Errorf("unknown storage %q", name)
	}

	if c.sharedMem.isRelevant(c.ttl, *specific) {
		if (*c.sharedMem)[*specific].Alive {
			return true, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in memory cache: %s", specific.Name)
	}

	var oldFile storageStatuses
	if c.sharedFileUse {
		oldFile, err = readFile(c.sharedFilePath)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	if oldFile.isRelevant(c.ttl, *specific) {
		(*c.sharedMem)[*specific] = oldFile[*specific]
		if oldFile[*specific].Alive {
			tracelog.InfoLogger.Printf("Storage is alive in file cache: %s", specific.Name)
			return true, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in file cache: %s", specific.Name)
	} else {
		tracelog.InfoLogger.Printf("Storage status is outdated in file cache: %s", specific.Name)
	}

	tracelog.InfoLogger.Printf("Checking for alive again: %s", specific.Name)

	checkResult := c.checkForAlive(*specific)

	newFile := updateFileContent(oldFile, checkResult)
	c.storeStatuses(newFile)

	if (*c.sharedMem)[*specific].Alive {
		tracelog.InfoLogger.Printf("Storage is alive: %s", specific.Name)
		return true, nil
	}
	tracelog.InfoLogger.Printf("Storage is dead: %s", specific.Name)
	return false, nil
}

func (c *statusCache) storeStatuses(new storageStatuses) {
	if c.sharedFileUse {
		err := writeFile(c.sharedFilePath, new)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to write cache file, each subsequent command will check the storages again: %v", err)
		}
	}

	c.sharedMem = &new
}

func (c *statusCache) checkForAlive(storageKeys ...Key) map[Key]bool {
	nameResult := c.checker.CheckForAlive(storageNames(storageKeys)...)
	keyResult := make(map[Key]bool, len(nameResult))
	for _, key := range storageKeys {
		if res, ok := nameResult[key.Name]; ok {
			keyResult[key] = res
		}
	}
	return keyResult
}

func storageNames(keys []Key) []string {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.Name
	}
	return names
}
