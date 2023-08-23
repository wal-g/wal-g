package cache

import (
	"sync"
	"time"
)

// memCache is stored in memory and therefore shared within a single WAL-G process.
var memCache storageStatuses
var memCacheMu *sync.Mutex

func init() {
	memCache = map[key]status{}
	memCacheMu = new(sync.Mutex)
}

func (ss storageStatuses) isRelevant(ttl time.Duration, storages ...NamedFolder) bool {
	if len(ss) == 0 {
		return false
	}
	for _, s := range storages {
		status, cached := ss[s.Key]
		if !cached {
			return false
		}
		if time.Since(status.Checked) > ttl {
			return false
		}
	}
	return true
}

func (ss storageStatuses) splitByRelevance(ttl time.Duration, storages []NamedFolder) (
	relevant []NamedFolder,
	outdated []NamedFolder,
) {
	relevanceMap := make(map[key]bool, len(ss))
	for key, status := range ss {
		checkedRecently := time.Since(status.Checked) <= ttl
		relevanceMap[key] = checkedRecently
	}

	for _, s := range storages {
		if relevanceMap[s.Key] {
			relevant = append(relevant, s)
		} else {
			outdated = append(outdated, s)
		}
	}
	return relevant, outdated
}

func (ss storageStatuses) getAllAlive(storagesInOrder []NamedFolder) []NamedFolder {
	var alive []NamedFolder
	for _, s := range storagesInOrder {
		status, cached := ss[s.Key]
		if !cached {
			continue
		}
		if status.Alive {
			alive = append(alive, s)
		}
	}
	return alive
}

// getRelevantFirstAlive provides
func (ss storageStatuses) getRelevantFirstAlive(ttl time.Duration, storagesInOrder []NamedFolder) (
	firstAlive *NamedFolder,
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
		if storagesInOrder[i].Key != relevant[i].Key {
			break
		}
		if ss[relevant[i].Key].Alive {
			firstAlive = &relevant[i]
			break
		}
	}
	return firstAlive, allRelevant
}
