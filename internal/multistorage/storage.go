package multistorage

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/internal/multistorage/stats/cache"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Storage = &Storage{}

type Storage struct {
	statsCollector   stats.Collector
	specificStorages []NamedStorage
	rootFolder       storage.Folder
}

type Config struct {
	AliveChecks          bool
	AliveCheckTimeout    time.Duration
	AliveCheckWriteBytes uint
	CheckWrite           bool
	StatusCache          *cache.Config
}

func NewStorage(config *Config, primary storage.HashableStorage, failovers map[string]storage.HashableStorage) (*Storage, error) {
	specificStorages := NameAndOrderStorages(primary, failovers)
	statsCollector, err := configureStatsCollector(specificStorages, config)
	if err != nil {
		return nil, fmt.Errorf("configure stats collector: %w", err)
	}
	rootFolder := NewFolder(specificStorages.RootFolders(), statsCollector)

	return &Storage{
		statsCollector:   statsCollector,
		specificStorages: specificStorages,
		rootFolder:       rootFolder,
	}, nil
}

func configureStatsCollector(storages NamedStorages, config *Config) (stats.Collector, error) {
	switch {
	case config.AliveChecks && config.CheckWrite:
		statusCache, err := cache.New(storages.Keys(), config.StatusCache, cache.DefaultRWMem, cache.DefaultRWFile)
		if err != nil {
			return nil, fmt.Errorf("init storage status cache: %w", err)
		}
		aliveChecker := stats.NewRWAliveChecker(storages.RootFolders(), config.AliveCheckTimeout, config.AliveCheckWriteBytes)
		return stats.NewCollector(storages.Names(), statusCache, aliveChecker), nil

	case config.AliveChecks && !config.CheckWrite:
		statusCache, err := cache.New(storages.Keys(), config.StatusCache, cache.DefaultROMem, cache.DefaultROFile)
		if err != nil {
			return nil, fmt.Errorf("init storage status cache: %w", err)
		}
		aliveChecker := stats.NewROAliveChecker(storages.RootFolders(), config.AliveCheckTimeout)
		return stats.NewCollector(storages.Names(), statusCache, aliveChecker), nil

	case !config.AliveChecks:
		return stats.NewNopCollector(storages.Names()), nil

	default:
		tracelog.ErrorLogger.Printf("Failed to init multi-storage stats collector: nop collector will be used")
		return stats.NewNopCollector(storages.Names()), nil
	}
}

func (s *Storage) RootFolder() storage.Folder {
	return s.rootFolder
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
	err := s.statsCollector.Close()
	if err != nil {
		closErr.Add(fmt.Errorf("close storage stats collector: %w", err))
	}
	if !closErr.Empty() {
		return closErr
	}
	return nil
}

type CloseError struct {
	specificStorageErrs []error
}

func (ce *CloseError) Empty() bool {
	return len(ce.specificStorageErrs) == 0
}

func (ce *CloseError) Add(err error) {
	ce.specificStorageErrs = append(ce.specificStorageErrs, err)
}

func (ce *CloseError) Error() string {
	errMsgs := make([]string, 0, len(ce.specificStorageErrs))
	for _, e := range ce.specificStorageErrs {
		errMsgs = append(errMsgs, e.Error())
	}
	return fmt.Sprintf("failed to close storage(s) and release resources: %s", strings.Join(errMsgs, ", "))
}

func NameAndOrderStorages(
	primary storage.HashableStorage,
	failover map[string]storage.HashableStorage,
) NamedStorages {
	var namedFailovers NamedStorages
	for name, fo := range failover {
		namedFailovers = append(namedFailovers, NamedStorage{
			Key:             cache.Key{Name: name, Hash: fo.ConfigHash()},
			Name:            name,
			HashableStorage: fo,
		})
	}
	sort.Slice(namedFailovers, func(i, j int) bool { return namedFailovers[i].Key.String() < namedFailovers[j].Key.String() })

	return append(
		NamedStorages{
			{
				Key:             cache.Key{Name: consts.DefaultStorage, Hash: primary.ConfigHash()},
				Name:            consts.DefaultStorage,
				HashableStorage: primary,
			},
		},
		namedFailovers...,
	)
}

type NamedStorage struct {
	// Key is unique among all WAL-G configurations
	Key cache.Key

	// Name is unique among a single WAL-G configuration
	Name string

	storage.HashableStorage
}

type NamedStorages []NamedStorage

func (ns NamedStorages) Names() []string {
	res := make([]string, len(ns))
	for i, s := range ns {
		res[i] = s.Name
	}
	return res
}

func (ns NamedStorages) Keys() map[string]cache.Key {
	res := make(map[string]cache.Key, len(ns))
	for _, s := range ns {
		res[s.Name] = s.Key
	}
	return res
}

func (ns NamedStorages) RootFolders() map[string]storage.Folder {
	specificFolders := make(map[string]storage.Folder, len(ns))
	for _, specSt := range ns {
		specificFolders[specSt.Name] = specSt.RootFolder()
	}
	return specificFolders
}
