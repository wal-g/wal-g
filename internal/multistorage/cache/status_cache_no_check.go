package cache

import (
	"fmt"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type statusCacheNoCheck struct {
	storagesInOrder []NamedFolder
}

func NewStatusCacheNoCheck(
	primary storage.Folder,
	failover map[string]storage.Folder,
) (StatusCache, error) {
	storagesInOrder, err := NameAndOrderFolders(primary, failover)
	if err != nil {
		return &statusCacheNoCheck{}, err
	}

	return &statusCacheNoCheck{
		storagesInOrder: storagesInOrder,
	}, nil
}

func (c *statusCacheNoCheck) AllAliveStorages() ([]NamedFolder, error) {
	return c.storagesInOrder, nil
}

func (c *statusCacheNoCheck) FirstAliveStorage() (*NamedFolder, error) {
	return &c.storagesInOrder[0], nil
}

func (c *statusCacheNoCheck) SpecificStorage(name string) (*NamedFolder, error) {
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
	return specificStorage, nil
}
