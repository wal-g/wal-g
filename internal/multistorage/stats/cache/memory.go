package cache

import "sync"

type SharedMemory struct {
	Statuses storageStatuses
	*sync.Mutex
}

func NewSharedMemory() *SharedMemory {
	return &SharedMemory{
		Statuses: storageStatuses{},
		Mutex:    new(sync.Mutex),
	}
}
