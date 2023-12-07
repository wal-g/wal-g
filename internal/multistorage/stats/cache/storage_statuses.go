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
type Aliveness int

type storageStatus struct {
	// PotentialAliveness is the maximum Aliveness value, as if all operations with this storage were successful.
	PotentialAliveness Aliveness `json:"potential_aliveness"`

	// ActualAliveness is the actual Aliveness value, that depends on the success of performed operations.
	// If it is equal to PotentialAliveness, the storage is fully alive. If it is 0, the storage is fully dead.
	ActualAliveness Aliveness `json:"actual_aliveness"`

	Updated time.Time `json:"updated"`
}

const (
	aliveLimitNumerator   = 95
	aliveLimitDenominator = 100
)

func (s storageStatus) alive() bool {
	return s.ActualAliveness >= s.PotentialAliveness*aliveLimitNumerator/aliveLimitDenominator
}

func (s storageStatus) applyExplicitCheckResult(alive bool, checkTime time.Time) storageStatus {
	if alive {
		return storageStatus{
			PotentialAliveness: s.PotentialAliveness,
			ActualAliveness:    s.PotentialAliveness,
			Updated:            checkTime,
		}
	} else {
		return storageStatus{
			PotentialAliveness: s.PotentialAliveness,
			ActualAliveness:    0,
			Updated:            checkTime,
		}
	}
}

func (s storageStatus) applyOperationResult(alive bool, weight int, checkTime time.Time) storageStatus {
	if alive {
		return storageStatus{
			PotentialAliveness: expMovingAverage(s.PotentialAliveness, weight),
			ActualAliveness:    expMovingAverage(s.ActualAliveness, weight),
			Updated:            checkTime,
		}
	} else {
		return storageStatus{
			PotentialAliveness: expMovingAverage(s.PotentialAliveness, weight),
			ActualAliveness:    expMovingAverage(s.ActualAliveness, 0),
			Updated:            checkTime,
		}
	}
}

const (
	emaAlphaNumerator       = 875
	emaAlphaDenominator     = 1000
	ema1MinusAlphaNumerator = emaAlphaDenominator - emaAlphaNumerator
)

func expMovingAverage(prevAverage Aliveness, newValue int) Aliveness {
	return (prevAverage * emaAlphaNumerator / emaAlphaDenominator) +
		(Aliveness(newValue) * ema1MinusAlphaNumerator / emaAlphaDenominator)
}

type storageStatuses map[Key]storageStatus

func (ss storageStatuses) applyExplicitCheckResult(checkResult map[Key]bool) storageStatuses {
	newStatuses := make(storageStatuses, len(ss))
	for key, status := range ss {
		newStatuses[key] = status
	}

	checkTime := time.Now()
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
		if status.alive() {
			aliveMap[key.Name] = true
		} else {
			aliveMap[key.Name] = false
		}
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
		if b[key].Updated.After(result[key].Updated) {
			result[key] = b[key]
		}
	}
	return result
}
