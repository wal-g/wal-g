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
) StatusCache {
	return &statusCache{
		storagesInOrder: nameAndOrderFolders(primary, failover),
		ttl:             ttl,
		checkTimeout:    checkTimeout,
	}
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

	memFirstAlive := memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if memFirstAlive != nil {
		return memFirstAlive, nil
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	fileFirstAlive := oldFile.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if fileFirstAlive != nil {
		memCache[fileFirstAlive.Name] = oldFile[fileFirstAlive.Name]
		return fileFirstAlive, nil
	}

	_, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)

	checkResult := checkForAlive(c.checkTimeout, outdated...)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder), nil
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
		if statuses[specificStorage.Name].Alive {
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
		memCache[specificStorage.Name] = oldFile[specificStorage.Name]
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
	Name string
	storage.Folder
}

func nameAndOrderFolders(primary storage.Folder, failover map[string]storage.Folder) []NamedFolder {
	var failoverFolders []NamedFolder
	for name, folder := range failover {
		failoverFolders = append(failoverFolders, NamedFolder{
			Name:   name,
			Folder: folder,
		})
	}
	sort.Slice(failoverFolders, func(i, j int) bool { return failoverFolders[i].Name < failoverFolders[j].Name })

	return append(
		[]NamedFolder{
			{
				Name:   "default",
				Folder: primary,
			},
		},
		failoverFolders...,
	)
}
