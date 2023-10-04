package cache

import (
	"fmt"
	"sort"
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
}

func NewStatusCache(
	primary storage.Folder,
	failover map[string]storage.Folder,
	ttl time.Duration,
	checker AliveChecker,
) (StatusCache, error) {
	storagesInOrder, err := NameAndOrderFolders(primary, failover)
	if err != nil {
		return &statusCache{}, err
	}

	return &statusCache{
		storagesInOrder: storagesInOrder,
		ttl:             ttl,
		checker:         checker,
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
		allAlive := memCache.getAllAlive(c.storagesInOrder)
		tracelog.InfoLogger.Printf("Take all alive storages from file cache: %v", storageNames(allAlive))
		return allAlive, nil
	}

	tracelog.InfoLogger.Printf("Storage status cache is outdated, checking for alive again: %v", storageNames(outdated))

	checkResult := c.checker.checkForAlive(outdated...)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to write cache file, each subsequent command will check the storages again: %v", err)
	}

	memCache = newFile
	allAlive := memCache.getAllAlive(c.storagesInOrder)
	tracelog.InfoLogger.Printf("Found alive storages: %v", storageNames(allAlive))
	return allAlive, nil
}

func (c *statusCache) FirstAliveStorage() (*NamedFolder, error) {
	memCacheMu.Lock()
	defer memCacheMu.Unlock()

	memFirstAlive, allRelevant := memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if memFirstAlive != nil {
		return memFirstAlive, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There is no alive storages in memory cache")
		return nil, nil
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	fileFirstAlive, allRelevant := oldFile.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if fileFirstAlive != nil {
		tracelog.InfoLogger.Printf("Take first alive storage from file cache: %s", fileFirstAlive.Name)
		memCache[fileFirstAlive.Key] = oldFile[fileFirstAlive.Key]
		return fileFirstAlive, nil
	}
	if allRelevant {
		tracelog.InfoLogger.Print("There is no alive storages in file cache")
		return nil, nil
	}

	_, outdated := oldFile.splitByRelevance(c.ttl, c.storagesInOrder)

	tracelog.InfoLogger.Printf("Storage status cache is outdated, checking for alive again: %v", storageNames(outdated))

	checkResult := c.checker.checkForAlive(outdated...)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to write cache file, each subsequent command will check the storages again: %v", err)
	}

	memCache = newFile
	firstAlive, _ := memCache.getRelevantFirstAlive(c.ttl, c.storagesInOrder)
	if firstAlive == nil {
		tracelog.InfoLogger.Print("Found no alive storages")
	} else {
		tracelog.InfoLogger.Printf("First found alive storage: %s", firstAlive.Name)
	}
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

	if memCache.isRelevant(c.ttl, *specificStorage) {
		if memCache[specificStorage.Key].Alive {
			return specificStorage, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in memory cache: %s", specificStorage.Name)
		return nil, nil
	}

	oldFile, err := readFile()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to read cache file, it will be overwritten: %v", err)
	}
	if oldFile.isRelevant(c.ttl, *specificStorage) {
		memCache[specificStorage.Key] = oldFile[specificStorage.Key]
		if oldFile[specificStorage.Key].Alive {
			tracelog.InfoLogger.Printf("Storage is alive in file cache: %s", specificStorage.Name)
			return specificStorage, nil
		}
		tracelog.InfoLogger.Printf("Storage is dead in file cache: %s", specificStorage.Name)
		return nil, nil
	}

	tracelog.InfoLogger.Printf("Storage status cache is outdated, checking for alive again: %s", specificStorage.Name)

	checkResult := c.checker.checkForAlive(*specificStorage)

	newFile := updateFileContent(oldFile, checkResult)
	err = writeFile(newFile)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to write cache file, each subsequent command will check the storages again: %v", err)
	}

	memCache = newFile
	if memCache[specificStorage.Key].Alive {
		tracelog.InfoLogger.Printf("Storage is alive: %s", specificStorage.Name)
		return specificStorage, nil
	}
	tracelog.InfoLogger.Printf("Storage is dead: %s", specificStorage.Name)
	return nil, nil
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
