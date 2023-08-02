package cache

import (
	"sync"
	"time"
)

// memCache is stored in memory and therefore shared within a single WAL-G process.
var memCache storageStatuses
var memCacheMu *sync.Mutex

func init() {
	memCache = map[string]status{}
	memCacheMu = new(sync.Mutex)
}

func (ss storageStatuses) isRelevant(ttl time.Duration, storages ...NamedFolder) bool {
	if len(ss) == 0 {
		return false
	}
	for _, s := range storages {
		status, cached := ss[s.Name]
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
	relevantMap := make(map[string]bool, len(ss))
	for name, status := range ss {
		checkedRecently := time.Since(status.Checked) <= ttl
		relevantMap[name] = checkedRecently
	}

	for _, s := range storages {
		if relevantMap[s.Name] {
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
		status, cached := ss[s.Name]
		if !cached {
			continue
		}
		if status.Alive {
			alive = append(alive, s)
		}
	}
	return alive
}

func (ss storageStatuses) getRelevantFirstAlive(ttl time.Duration, storagesInOrder []NamedFolder) *NamedFolder {
	var firstAlive *NamedFolder
	relevant, _ := ss.splitByRelevance(ttl, storagesInOrder)
	for i := range storagesInOrder {
		if i >= len(relevant) {
			break
		}
		if storagesInOrder[i].Name != relevant[i].Name {
			break
		}
		if ss[relevant[i].Name].Alive {
			firstAlive = &relevant[i]
			break
		}
	}
	return firstAlive
}
