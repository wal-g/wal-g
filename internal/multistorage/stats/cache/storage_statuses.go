package cache

import (
	"fmt"
	"strings"
	"time"
)

// Key identifies a storage and is unique within all WAL-G configurations, in contrast to Name, that is unique only
// within a single configuration. So, Key can be used to identify storages in cache files shared between several WAL-G
// configurations.
type Key struct {
	Name string
	Hash string
}

func ParseKey(str string) Key {
	delim := strings.LastIndex(str, "#")
	return Key{str[:delim], str[delim+1:]}
}

func (k Key) String() string {
	return fmt.Sprintf("%s#%s", k.Name, k.Hash)
}

// Aliveness measured in arbitrary units and shows how much a storage is alive. When some operation is performed with
// this storage, the metrica is increased or decreased based on the result of the operation and its weight.
type Aliveness float64

type storageStatus struct {
	// PotentialAliveness is the maximum Aliveness value, as if all operations with this storage were successful.
	PotentialAliveness Aliveness `json:"potential_aliveness"`

	// ActualAliveness is the actual Aliveness value, that depends on the success of performed operations.
	// If it is equal to PotentialAliveness, the storage is fully alive. If it is 0, the storage is fully dead.
	ActualAliveness Aliveness `json:"actual_aliveness"`

	// WasAlive is the previous storageStatus.alive() result, that was actual before the latest check.
	WasAlive bool `json:"previous_aliveness"`

	Updated time.Time `json:"updated"`
}

const (
	// aliveLimit is used to mark the storage alive when its Aliveness reaches this value
	aliveLimit Aliveness = 0.99

	// deadLimit is used to mark the storage dead when its Aliveness drops to this value
	deadLimit Aliveness = 0.05
)

func (s storageStatus) alive() bool {
	if s.PotentialAliveness == 0 {
		return s.WasAlive
	}
	if s.WasAlive {
		return s.ActualAliveness >= s.PotentialAliveness*deadLimit
	}
	return s.ActualAliveness >= s.PotentialAliveness*aliveLimit
}

func (s storageStatus) applyExplicitCheckResult(alive bool, checkTime time.Time) storageStatus {
	if alive {
		return storageStatus{
			PotentialAliveness: s.PotentialAliveness,
			ActualAliveness:    s.PotentialAliveness,
			WasAlive:           true,
			Updated:            checkTime,
		}
	}
	return storageStatus{
		PotentialAliveness: s.PotentialAliveness,
		ActualAliveness:    0,
		WasAlive:           false,
		Updated:            checkTime,
	}
}

func (s storageStatus) applyOperationResult(alive bool, weight float64, checkTime time.Time) storageStatus {
	if alive {
		return storageStatus{
			PotentialAliveness: expMovingAverage(s.PotentialAliveness, weight),
			ActualAliveness:    expMovingAverage(s.ActualAliveness, weight),
			WasAlive:           s.alive(),
			Updated:            checkTime,
		}
	}
	return storageStatus{
		PotentialAliveness: expMovingAverage(s.PotentialAliveness, weight),
		ActualAliveness:    expMovingAverage(s.ActualAliveness, 0),
		WasAlive:           s.alive(),
		Updated:            checkTime,
	}
}

// This exponential moving average α value allows a fully dead storage to become alive after 10 successful operations.
const emaAlpha = 0.6

func expMovingAverage(prevAverage Aliveness, newValue float64) Aliveness {
	return (prevAverage * emaAlpha) + (Aliveness(newValue) * (1 - emaAlpha))
}

type storageStatuses map[Key]storageStatus

func (ss storageStatuses) applyExplicitCheckResult(checkResult map[Key]bool, checkTime time.Time) storageStatuses {
	newStatuses := make(storageStatuses, len(ss))
	for key, status := range ss {
		newStatuses[key] = status
	}

	for key, alive := range checkResult {
		newStatuses[key] = newStatuses[key].applyExplicitCheckResult(alive, checkTime)
	}

	return newStatuses
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
		if time.Since(status.Updated) > ttl {
			return false
		}
	}
	return true
}

func (ss storageStatuses) splitByRelevance(ttl time.Duration, storages []Key) (
	relevant storageStatuses,
	outdated storageStatuses,
) {
	relevanceMap := make(map[Key]bool, len(ss))
	for key, status := range ss {
		recentlyUpdated := time.Since(status.Updated) <= ttl
		relevanceMap[key] = recentlyUpdated
	}

	relevant, outdated = storageStatuses{}, storageStatuses{}
	for _, key := range storages {
		if relevanceMap[key] {
			relevant[key] = ss[key]
		} else {
			outdated[key] = ss[key]
		}
	}
	return relevant, outdated
}

func (ss storageStatuses) filter(storages []Key) storageStatuses {
	result := storageStatuses{}
	for _, key := range storages {
		if status, ok := ss[key]; ok {
			result[key] = status
		}
	}
	return result
}

func (ss storageStatuses) aliveMap() AliveMap {
	aliveMap := make(AliveMap, len(ss))
	for key, status := range ss {
		aliveMap[key.Name] = status.alive()
	}
	return aliveMap
}

func mergeByRelevance(a, b storageStatuses) storageStatuses {
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	result := make(storageStatuses, maxLen)
	for key, status := range a {
		result[key] = status
	}
	for key := range b {
		_, ok := result[key]
		if !ok || b[key].Updated.After(result[key].Updated) {
			result[key] = b[key]
		}
	}
	return result
}
