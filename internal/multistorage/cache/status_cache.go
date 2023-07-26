package cache

import (
	"fmt"
	"sort"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type StatusCache struct {
	storagesInOrder []NamedFolder
	ttl             time.Duration
	checkTimeout    time.Duration
}

func NewStatusCache(
	primary storage.Folder,
	failover map[string]storage.Folder,
	ttl, checkTimeout time.Duration,
) *StatusCache {
	return &StatusCache{
		storagesInOrder: nameAndOrderFolders(primary, failover),
		ttl:             ttl,
		checkTimeout:    checkTimeout,
	}
}

func (c *StatusCache) AllAliveStorages() ([]NamedFolder, error) {
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

	checkResult, err := checkForAlive(c.checkTimeout, outdated...)
	if err != nil {
		return nil, fmt.Errorf("find alive storages: %w", err)
	}

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getAllAlive(c.storagesInOrder), nil
}

func (c *StatusCache) FirstAliveStorage() (*NamedFolder, error) {
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

	checkResult, err := checkForAlive(c.checkTimeout, outdated...)
	if err != nil {
		return nil, fmt.Errorf("find alive storages: %w", err)
	}

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder), nil
}

func (c *StatusCache) SpecificStorage(name string) (specificStorage NamedFolder, err error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	var found bool
	for _, s := range c.storagesInOrder {
		if s.Name == name {
			specificStorage = s
			found = true
		}
	}
	if !found {
		return NamedFolder{}, fmt.Errorf("unknown storage %q", name)
	}

	ensureStorageIsAlive := func(statuses storageStatuses) (NamedFolder, error) {
		if statuses[specificStorage.Name].Alive {
			return specificStorage, nil
		}
		return NamedFolder{}, fmt.Errorf("storage %q is dead", name)
	}

	if memCache.isRelevant(c.ttl, specificStorage) {
		return ensureStorageIsAlive(memCache)
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	if oldFile.isRelevant(c.ttl, specificStorage) {
		memCache[specificStorage.Name] = oldFile[specificStorage.Name]
		return ensureStorageIsAlive(oldFile)
	}

	checkResult, err := checkForAlive(c.checkTimeout, specificStorage)
	if err != nil {
		return NamedFolder{}, fmt.Errorf("check if storage %q is alive", specificStorage.Name)
	}

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return NamedFolder{}, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return ensureStorageIsAlive(memCache)
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
