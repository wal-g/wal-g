package cache

import (
	"fmt"
	"sort"
	"time"

	"github.com/wal-g/tracelog"
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
	checkTimeout    time.Duration
}

func NewStatusCache(
	primary storage.Folder,
	failover map[string]storage.Folder,
	ttl, checkTimeout time.Duration,
) (StatusCache, error) {
	storagesInOrder, err := nameAndOrderFolders(primary, failover)
	if err != nil {
		return &statusCache{}, err
	}
	return &statusCache{
		storagesInOrder: storagesInOrder,
		ttl:             ttl,
		checkTimeout:    checkTimeout,
	}, nil
}

func (c *statusCache) AllAliveStorages() ([]NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	if memCache.isRelevant(c.ttl, c.storagesInOrder...) {
		return memCache.getAllAlive(c.storagesInOrder), nil
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	_, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)
	if len(outdated) == 0 {
		memCache = oldFile
		return memCache.getAllAlive(c.storagesInOrder), nil
	}

	checkResult := checkForAlive(c.checkTimeout, outdated...)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getAllAlive(c.storagesInOrder), nil
}

func (c *statusCache) FirstAliveStorage() (*NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	memFirstAlive, allRelevant := memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if memFirstAlive != nil {
		return memFirstAlive, nil
	}
	if allRelevant {
		return nil, nil
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	fileFirstAlive, allRelevant := oldFile.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if fileFirstAlive != nil {
		memCache[fileFirstAlive.Key] = oldFile[fileFirstAlive.Key]
		return fileFirstAlive, nil
	}
	if allRelevant {
		return nil, nil
	}

	_, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)

	checkResult := checkForAlive(c.checkTimeout, outdated...)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	firstAlive, _ := memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	return firstAlive, nil
}

func (c *statusCache) SpecificStorage(name string) (*NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	var specificStorage *NamedFolder
	for _, s := range c.storagesInOrder {
		if s.Name == name {
			sCpy := s
			specificStorage = &sCpy
			break
		}
	}
	if specificStorage == nil {
		return nil, fmt.Errorf("unknown storage %q", name)
	}

	getStorageIfAlive := func(statuses storageStatuses) (*NamedFolder, error) {
		if statuses[specificStorage.Key].Alive {
			return specificStorage, nil
		}
		return nil, nil
	}

	if memCache.isRelevant(c.ttl, *specificStorage) {
		return getStorageIfAlive(memCache)
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	if oldFile.isRelevant(c.ttl, *specificStorage) {
		memCache[specificStorage.Key] = oldFile[specificStorage.Key]
		return getStorageIfAlive(oldFile)
	}

	checkResult := checkForAlive(c.checkTimeout, *specificStorage)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return getStorageIfAlive(memCache)
}

type NamedFolder struct {
	Key  key
	Name string
	storage.Folder
}

func nameAndOrderFolders(primary storage.Folder, failover map[string]storage.Folder) ([]NamedFolder, error) {
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
			Folder: folder,
		})
	}
	sort.Slice(failoverFolders, func(i, j int) bool { return failoverFolders[i].Key < failoverFolders[j].Key })

	return append(
		[]NamedFolder{
			{
				Key:    formatKey("default", hashablePrimary.Hash()),
				Name:   "default",
				Folder: primary,
			},
		},
		failoverFolders...,
	), nil
}
