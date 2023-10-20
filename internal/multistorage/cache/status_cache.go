package cache

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

//go:generate mockgen -source status_cache.go -destination status_cache_mock.go -package cache
type StatusCache interface {
	AllAliveStorages() ([]NamedFolder, error)
	FirstAliveStorage() (*NamedFolder, error)
	SpecificStorage(name string) (*NamedFolder, error)
}

type statusCache struct {
	storagesInOrder []NamedFolder
	ttl             time.Duration
	checker         AliveChecker

	// sharedMem keeps the intra-process cache that's shared between different threads and subsequent storages requests.
	sharedMem   *storageStatuses
	sharedMemMu *sync.Mutex

	// sharedFile keeps the inter-process cache that's shared between different command executions.
	sharedFile    string
	sharedFileUse bool
}

type StatusCacheOpt func(c *statusCache)

func WithSharedMemory(mem *storageStatuses, memMu *sync.Mutex) StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedMem = mem
		c.sharedMemMu = memMu
	}
}

func WithSharedFile(file string) StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedFile = file
		c.sharedFileUse = true
	}
}

func WithoutSharedFile() StatusCacheOpt {
	return func(c *statusCache) {
		c.sharedFileUse = false
	}
}

func NewStatusCache(
	primary storage.Folder,
	failover map[string]storage.Folder,
	ttl time.Duration,
	checker AliveChecker,
	opts ...StatusCacheOpt,
) (StatusCache, error) {
	storagesInOrder, err := NameAndOrderFolders(primary, failover)
	if err != nil {
		return &statusCache{}, err
	}

	homeFileUse := true
	homeFile, err := HomeStatusFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Can't use storage status cache file from $HOME: %v", err)
		homeFileUse = false
	}

	c := &statusCache{
		storagesInOrder: storagesInOrder,
		ttl:             ttl,
		checker:         checker,

		sharedMem:     &globalMemCache,
		sharedMemMu:   globalMemCacheMu,
		sharedFile:    homeFile,
		sharedFileUse: homeFileUse,
	}

	for _, o := range opts {
		o(c)
	}

	return c, nil
}

func (c *statusCache) AllAliveStorages() (allAlive []NamedFolder, err error) {
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
		oldFile, err = readFile(c.sharedFile)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	relevant, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)
	if len(outdated) == 0 {
		c.sharedMem = &oldFile
		allAlive = c.sharedMem.getAllAlive(c.storagesInOrder)
		if len(allAlive) > 0 {
			tracelog.InfoLogger.Printf("Take all alive storages from file cache: %v", storageNames(allAlive))
			return allAlive, nil
		}
	}

	newFile := oldFile
	if len(outdated) > 0 {
		tracelog.InfoLogger.Printf(
			"Storage status cache is outdated, checking for alive again: %v",
			storageNames(outdated),
		)
		checkResult := c.checker.checkForAlive(outdated...)
		newFile = updateFileContent(oldFile, checkResult)
		allAlive = newFile.getAllAlive(c.storagesInOrder)
	}

	defer func() {
		if c.sharedFileUse {
			err := writeFile(c.sharedFile, newFile)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to write cache file, each subsequent command will check the storages again: %v",
					err,
				)
			}
		}
		c.sharedMem = &newFile
		tracelog.InfoLogger.Printf("Found alive storages: %v", storageNames(allAlive))
	}()

	if len(allAlive) > 0 {
		return allAlive, nil
	}

	if len(relevant) > 0 {
		tracelog.InfoLogger.Printf(
			"All storages are dead in cache, rechecking relevant ones: %v",
			storageNames(relevant),
		)
		checkResult := c.checker.checkForAlive(relevant...)
		newFile = updateFileContent(newFile, checkResult)
		allAlive = newFile.getAllAlive(c.storagesInOrder)
	}

	return allAlive, nil
}

func (c *statusCache) FirstAliveStorage() (firstAlive *NamedFolder, err error) {
	c.sharedMemMu.Lock()
	defer c.sharedMemMu.Unlock()

	memFirstAlive, allRelevant := c.sharedMem.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if memFirstAlive != nil {
		return memFirstAlive, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There are no alive storages in memory cache")
	}

	var oldFile storageStatuses
	if c.sharedFileUse {
		oldFile, err = readFile(c.sharedFile)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	fileFirstAlive, allRelevant := oldFile.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if fileFirstAlive != nil {
		tracelog.InfoLogger.Printf("Take first alive storage from file cache: %s", fileFirstAlive.Name)
		(*c.sharedMem)[fileFirstAlive.Key] = oldFile[fileFirstAlive.Key]
		return fileFirstAlive, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There are no alive storages in file cache")
	}

	relevant, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)

	newFile := oldFile
	if len(outdated) > 0 {
		tracelog.InfoLogger.Printf("Storage status cache is outdated, checking for alive again: %v", storageNames(outdated))
		checkResult := c.checker.checkForAlive(outdated...)
		newFile = updateFileContent(newFile, checkResult)
		firstAlive, _ = newFile.getRelevantFirstAlive(time.Duration(math.MaxInt64), c.storagesInOrder)
	}

	defer func() {
		if c.sharedFileUse {
			err := writeFile(c.sharedFile, newFile)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to write cache file, each subsequent command will check the storages again: %v",
					err,
				)
			}
		}
		c.sharedMem = &newFile
		if firstAlive == nil {
			tracelog.InfoLogger.Print("Found no alive storages")
		} else {
			tracelog.InfoLogger.Printf("First found alive storage: %s", firstAlive.Name)
		}
	}()

	if firstAlive != nil {
		return firstAlive, nil
	}

	if len(relevant) > 0 {
		tracelog.InfoLogger.Printf("All storages are dead in cache, rechecking relevant ones: %v", storageNames(relevant))
		checkResult := c.checker.checkForAlive(relevant...)
		newFile = updateFileContent(newFile, checkResult)
		firstAlive, _ = newFile.getRelevantFirstAlive(time.Duration(math.MaxInt64), c.storagesInOrder)
	}

	return firstAlive, nil
}

func (c *statusCache) SpecificStorage(name string) (specific *NamedFolder, err error) {
	c.sharedMemMu.Lock()
	defer c.sharedMemMu.Unlock()

	specific = storageWithName(c.storagesInOrder, name)
	if specific == nil {
		return nil, fmt.Errorf("unknown storage %q", name)
	}

	if c.sharedMem.isRelevant(c.ttl, *specific) {
		if (*c.sharedMem)[specific.Key].Alive {
			return specific, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in memory cache: %s", specific.Name)
	}

	var oldFile storageStatuses
	if c.sharedFileUse {
		oldFile, err = readFile(c.sharedFile)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
		}
	}
	if oldFile.isRelevant(c.ttl, *specific) {
		(*c.sharedMem)[specific.Key] = oldFile[specific.Key]
		if oldFile[specific.Key].Alive {
			tracelog.InfoLogger.Printf("Storage is alive in file cache: %s", specific.Name)
			return specific, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in file cache: %s", specific.Name)
	} else {
		tracelog.InfoLogger.Printf("Storage status is outdated in file cache: %s", specific.Name)
	}

	tracelog.InfoLogger.Printf("Checking for alive again: %s", specific.Name)

	checkResult := c.checker.checkForAlive(*specific)

	newFile := updateFileContent(oldFile, checkResult)
	if c.sharedFileUse {
		err = writeFile(c.sharedFile, newFile)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to write cache file, each subsequent command will check the storages again: %v", err)
		}
	}

	c.sharedMem = &newFile
	if (*c.sharedMem)[specific.Key].Alive {
		tracelog.InfoLogger.Printf("Storage is alive: %s", specific.Name)
		return specific, nil
	}
	tracelog.InfoLogger.Printf("Storage is dead: %s", specific.Name)
	return nil, nil
}

func storageWithName(storages []NamedFolder, name string) *NamedFolder {
	for _, s := range storages {
		if s.Name == name {
			sCpy := s
			return &sCpy
		}
	}
	return nil
}

func storageNames(folders []NamedFolder) []string {
	names := make([]string, len(folders))
	for i, f := range folders {
		names[i] = f.Name
	}
	return names
}

type NamedFolder struct {
	Key  key
	Name string
	Root string
	storage.Folder
}

func NameAndOrderFolders(primary storage.Folder, failover map[string]storage.Folder) ([]NamedFolder, error) {
	hashablePrimary, ok := primary.(storage.HashableFolder)
	if !ok {
		return nil, fmt.Errorf("storage \"default\" must be hashabe to be used in multi-storage folder")
	}

	var failoverFolders []NamedFolder
	for name, folder := range failover {
		hashableFolder, ok := folder.(storage.HashableFolder)
		if !ok {
			return nil, fmt.Errorf("storage %q must be hashable to be used in multi-storage folder", name)
		}
		failoverFolders = append(failoverFolders, NamedFolder{
			Key:    formatKey(name, hashableFolder.Hash()),
			Name:   name,
			Root:   folder.GetPath(),
			Folder: folder,
		})
	}
	sort.Slice(failoverFolders, func(i, j int) bool { return failoverFolders[i].Key < failoverFolders[j].Key })

	return append(
		[]NamedFolder{
			{
				Key:    formatKey(consts.DefaultStorage, hashablePrimary.Hash()),
				Name:   consts.DefaultStorage,
				Root:   primary.GetPath(),
				Folder: primary,
			},
		},
		failoverFolders...,
	), nil
}
