package cache

import (
	"fmt"
)

type statusCacheNoCheck struct {
	storagesInOrder []string
}

func NewStatusCacheNoCheck(storagesInOrder []Key) StatusCache {
	return &statusCacheNoCheck{
		storagesInOrder: storageNames(storagesInOrder),
	}
}

func (c *statusCacheNoCheck) AllAliveStorages() ([]string, error) {
	return c.storagesInOrder, nil
}

func (c *statusCacheNoCheck) FirstAliveStorage() (*string, error) {
	return &c.storagesInOrder[0], nil
}

func (c *statusCacheNoCheck) SpecificStorage(name string) (bool, error) {
	for _, s := range c.storagesInOrder {
		if s == name {
			return true, nil
		}
	}
	return false, fmt.Errorf("unknown storage %q", name)
}
