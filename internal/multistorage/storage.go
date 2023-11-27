package multistorage

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	statusCache      cache.StatusCache
	specificStorages []NamedStorage
	rootFolder       storage.Folder
}

type Config struct {
	AliveChecks          bool
	AliveCheckTimeout    time.Duration
	AliveCheckWriteBytes uint
	CheckWrite           bool
	StatusCacheTTL       time.Duration
}

func NewStorage(
	config *Config,
	primary storage.HashableStorage,
	failovers map[string]storage.HashableStorage,
) *Storage {
	specificStorages := NameAndOrderStorages(primary, failovers)

	specificFolders := make(map[string]storage.Folder, len(specificStorages))
	for _, specSt := range specificStorages {
		specificFolders[specSt.Name] = specSt.RootFolder()
	}

	var statusCache cache.StatusCache
	if config.AliveChecks {
		var aliveChecker cache.AliveChecker
		if config.CheckWrite {
			aliveChecker = cache.NewReadAliveChecker(specificFolders, config.AliveCheckTimeout)
		} else {
			aliveChecker = cache.NewRWAliveChecker(specificFolders, config.AliveCheckTimeout, config.AliveCheckWriteBytes)
		}
		statusCache = cache.NewStatusCache(storageKeys(specificStorages), config.StatusCacheTTL, aliveChecker)
	} else {
		statusCache = cache.NewStatusCacheNoCheck(storageKeys(specificStorages))
	}

	rootFolder := NewFolder(specificFolders, statusCache)

	return &Storage{
		statusCache:      statusCache,
		specificStorages: specificStorages,
		rootFolder:       rootFolder,
	}
}

func storageKeys(storages []NamedStorage) []cache.Key {
	res := make([]cache.Key, len(storages))
	for i, s := range storages {
		res[i] = s.Key
	}
	return res
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
}

func (s *Storage) SetRootFolder(folder storage.Folder) {
	s.rootFolder = folder
}

func (s *Storage) Close() error {
	if s == nil || len(s.specificStorages) == 0 {
		return nil
	}
	closErr := new(CloseError)
	for _, s := range s.specificStorages {
		err := s.Close()
		if err != nil {
			closErr.Add(fmt.Errorf("close %q: %w", s.Name, err))
		}
	}
	return closErr
}

type CloseError struct {
	specificStorageErrs []error
}

func (ce *CloseError) Add(err error) {
	ce.specificStorageErrs = append(ce.specificStorageErrs, err)
}

func (ce *CloseError) Error() string {
	errMsgs := make([]string, len(ce.specificStorageErrs))
	for _, e := range ce.specificStorageErrs {
		errMsgs = append(errMsgs, e.Error())
	}
	return fmt.Sprintf("failed to close storage(s) and release resources: %s", strings.Join(errMsgs, ", "))
}

func (s *Storage) SpecificStorages() []string {
	var names []string
	for _, specSt := range s.specificStorages {
		names = append(names, specSt.Name)
	}
	return names
}

type NamedStorage struct {
	// Key is unique among all WAL-G configurations
	Key cache.Key

	// Name is unique among a single WAL-G configuration
	Name string

	storage.HashableStorage
}

func NameAndOrderStorages(
	primary storage.HashableStorage,
	failover map[string]storage.HashableStorage,
) []NamedStorage {
	var namedFailovers []NamedStorage
	for name, fo := range failover {
		namedFailovers = append(namedFailovers, NamedStorage{
			Key:             cache.Key{Name: name, Hash: fo.ConfigHash()},
			Name:            name,
			HashableStorage: fo,
		})
	}
	sort.Slice(namedFailovers, func(i, j int) bool { return namedFailovers[i].Key.String() < namedFailovers[j].Key.String() })

	return append(
		[]NamedStorage{
			{
				Key:             cache.Key{Name: consts.DefaultStorage, Hash: primary.ConfigHash()},
				Name:            consts.DefaultStorage,
				HashableStorage: primary,
			},
		},
		namedFailovers...,
	)
}
