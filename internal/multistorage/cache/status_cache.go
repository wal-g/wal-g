package cache

import (
	"fmt"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type StatusCache struct {
	storages []NamedFolder
	ttl      time.Duration
}

func NewStatusCache(primary storage.Folder, failover map[string]storage.Folder, ttl time.Duration) *StatusCache {
	return &StatusCache{
		storages: nameFolders(primary, failover),
		ttl:      ttl,
	}
}

func (c *StatusCache) AllAliveStorages() ([]NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	if memCache.isRelevant(c.ttl, c.storages...) {
		return memCache.getAllAlive(c.storages), nil
	}

	oldFile, err := readFile()
	if err != nil {
		return nil, fmt.Errorf("read cache file: %w", err)
	}
	_, outdated := oldFile.splitByRelevance(c.ttl, c.storages)
	if len(outdated) == 0 {
		memCache = oldFile
		return memCache.getAllAlive(c.storages), nil
	}

	checkResult, err := checkForAlive(outdated...)
	if err != nil {
		return nil, fmt.Errorf("find alive storages: %w", err)
	}

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getAllAlive(c.storages), nil
}

func (c *StatusCache) FirstAliveStorage() (*NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	memFirstAlive := memCache.getRelevantFirstAlive(c.ttl, c.storages)
	if memFirstAlive != nil {
		return memFirstAlive, nil
	}

	oldFile, err := readFile()
	if err != nil {
		return nil, fmt.Errorf("read cache file: %w", err)
	}
	fileFirstAlive := oldFile.getRelevantFirstAlive(c.ttl, c.storages)
	if fileFirstAlive != nil {
		memCache[fileFirstAlive.Name] = oldFile[fileFirstAlive.Name]
		return fileFirstAlive, nil
	}

	_, outdated := oldFile.splitByRelevance(c.ttl, c.storages)

	checkResult, err := checkForAlive(outdated...)
	if err != nil {
		return nil, fmt.Errorf("find alive storages: %w", err)
	}

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		return nil, fmt.Errorf("write cache file: %w", err)
	}

	memCache = newFile
	return memCache.getRelevantFirstAlive(c.ttl, c.storages), nil
}

func (c *StatusCache) SpecificStorage(name string) (specificStorage NamedFolder, err error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	var found bool
	for _, s := range c.storages {
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
		return NamedFolder{}, fmt.Errorf("read cache file: %w", err)
	}
	if oldFile.isRelevant(c.ttl, specificStorage) {
		memCache[specificStorage.Name] = oldFile[specificStorage.Name]
		return ensureStorageIsAlive(oldFile)
	}

	checkResult, err := checkForAlive(specificStorage)
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

func nameFolders(primary storage.Folder, failover map[string]storage.Folder) []NamedFolder {
	namedFoldersInOrder := []NamedFolder{
		{
			Name:   "default",
			Folder: primary,
		},
	}
	for name, folder := range failover {
		namedFoldersInOrder = append(namedFoldersInOrder, NamedFolder{
			Name:   name,
			Folder: folder,
		})
	}
	return namedFoldersInOrder
}
