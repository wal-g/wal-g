package cache

import (
	"sync"
	"time"
)

// globalMemCache is the default in-memory cache that is shared within a single WAL-G process.
var globalMemCache storageStatuses
var globalMemCacheMu *sync.Mutex

func init() {
	globalMemCache = map[Key]status{}
	globalMemCacheMu = new(sync.Mutex)
}

func (ss storageStatuses) isRelevant(ttl time.Duration, storages ...Key) bool {
	if len(ss) == 0 {
		return false
	}
	for _, s := range storages {
		status, cached := ss[s]
		if !cached {
			return false
		}
		if time.Since(status.Checked) > ttl {
			return false
		}
	}
	return true
}

func (ss storageStatuses) splitByRelevance(ttl time.Duration, storages []Key) (
	relevant []Key,
	outdated []Key,
) {
	relevanceMap := make(map[Key]bool, len(ss))
	for key, status := range ss {
		checkedRecently := time.Since(status.Checked) <= ttl
		relevanceMap[key] = checkedRecently
	}

	for _, s := range storages {
		if relevanceMap[s] {
			relevant = append(relevant, s)
		} else {
			outdated = append(outdated, s)
		}
	}
	return relevant, outdated
}

func (ss storageStatuses) getAllAlive(storagesInOrder []Key) []string {
	var alive []string
	for _, s := range storagesInOrder {
		status, cached := ss[s]
		if !cached {
			continue
		}
		if status.Alive {
			alive = append(alive, s.Name)
		}
	}
	return alive
}

// getRelevantFirstAlive traverses storages in order of priority. If any relevant and alive storage is found, the
// traverse stops and this storage is returned. If any outdated storage is found before the first relevant and alive,
// nil is returned. If no alive storages are found, nil is returned as well.
func (ss storageStatuses) getRelevantFirstAlive(ttl time.Duration, storagesInOrder []Key) (
	firstAlive *Key,
	allRelevant bool,
) {
	relevant, _ := ss.splitByRelevance(ttl, storagesInOrder)
	if len(relevant) == len(storagesInOrder) {
		allRelevant = true
	}
	for i := range storagesInOrder {
		if i >= len(relevant) {
			break
		}
		if storagesInOrder[i] != relevant[i] {
			break
		}
		if ss[relevant[i]].Alive {
			firstAlive = &relevant[i]
			break
		}
	}
	return firstAlive, allRelevant
}
