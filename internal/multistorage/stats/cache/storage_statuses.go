package cache

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
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

func (s storageStatus) alive(p *EMAParams) bool {
	if s.WasAlive {
		return s.alivenessFactor(p) >= p.DeadLimit
	}
	return s.alivenessFactor(p) >= p.AliveLimit
}

func (s storageStatus) alivenessFactor(p *EMAParams) float64 {
	// In case the storage has no data, consider it having the minimum limit of aliveness.
	if s.hasNoData() {
		return p.DeadLimit
	}
	return float64(s.ActualAliveness / s.PotentialAliveness)
}

// hasNoData returns true if the storageStatus has just been initialized.
func (s storageStatus) hasNoData() bool {
	return s.PotentialAliveness == 0 && s.ActualAliveness == 0
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

func (s storageStatus) applyOperationResult(p *EMAParams, alive bool, weight float64, checkTime time.Time) storageStatus {
	alpha := s.calcEMAAlpha(p)
	tracelog.DebugLogger.Printf("Apply storage operation result with EMA alpha = %v", alpha)

	if alive {
		return storageStatus{
			PotentialAliveness: expMovingAverage(alpha, s.PotentialAliveness, weight),
			ActualAliveness:    expMovingAverage(alpha, s.ActualAliveness, weight),
			WasAlive:           s.alive(p),
			Updated:            checkTime,
		}
	}
	return storageStatus{
		PotentialAliveness: expMovingAverage(alpha, s.PotentialAliveness, weight),
		ActualAliveness:    expMovingAverage(alpha, s.ActualAliveness, 0),
		WasAlive:           s.alive(p),
		Updated:            checkTime,
	}
}

// calcEMAAlpha returns alpha from the allowed ranges, such that it is larger when aliveness is close to the limits, and
// smaller away from these limits.
func (s storageStatus) calcEMAAlpha(p *EMAParams) float64 {
	if s.alive(p) {
		// Take such EMA alpha value so that it is minimal with aliveness = deadLimit, and maximal with aliveness = 1.
		amplifier := (s.alivenessFactor(p) - p.DeadLimit) / (1 - p.DeadLimit)
		alphaRange := p.AlphaAlive
		return alphaRange.Max + (alphaRange.Min-alphaRange.Max)*amplifier
	}
	// Take such EMA alpha value so that it is minimal with aliveness = aliveLimit, and maximal with aliveness = 0.
	amplifier := s.alivenessFactor(p) / p.AliveLimit
	alphaRange := p.AlphaDead
	return alphaRange.Max + (alphaRange.Min-alphaRange.Max)*amplifier
}

func expMovingAverage(alpha float64, prevAverage Aliveness, newValue float64) Aliveness {
	return Aliveness((newValue * alpha) + (float64(prevAverage) * (1 - alpha)))
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

func (ss storageStatuses) aliveMap(p *EMAParams) AliveMap {
	aliveMap := make(AliveMap, len(ss))
	for key, status := range ss {
		aliveMap[key.Name] = status.alive(p)
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

// EMAParams are used by the Exponential Moving Average algorithm that makes the decision if a storage with a certain
// aliveness metric is alive or dead now.
type EMAParams struct {
	// AliveLimit is compared with the aliveness factor of a dead storage to decide if this storage should become alive.
	AliveLimit float64

	// DeadLimit is compared with the aliveness factor of an alive storage to decide if this storage should become dead.
	DeadLimit float64

	// AlphaAlive is the range of possible Alpha values for an alive storage.
	AlphaAlive EMAAlphaRange

	// AlphaDead is the range of possible Alpha values for a dead storage.
	AlphaDead EMAAlphaRange
}

// EMAAlphaRange specifies a minimum and a maximum value of Alpha used in the Exponential Moving Average algorithm.
type EMAAlphaRange struct {
	Min float64
	Max float64
}

func (p *EMAParams) Validate() error {
	ensureBetween0And1 := func(val float64, paramName string) error {
		if val <= 0 || val >= 1 {
			return fmt.Errorf("%s is expected to be in range (0, 1)", paramName)
		}
		return nil
	}
	err := ensureBetween0And1(p.AliveLimit, "alive limit")
	if err != nil {
		return err
	}
	err = ensureBetween0And1(p.DeadLimit, "dead limit")
	if err != nil {
		return err
	}
	if p.AliveLimit < p.DeadLimit {
		return errors.New("alive limit must be greater than dead limit")
	}
	err = ensureBetween0And1(p.AlphaAlive.Min, "max EMA alpha for alive storage")
	if err != nil {
		return err
	}
	err = ensureBetween0And1(p.AlphaAlive.Max, "min EMA alpha for alive storage")
	if err != nil {
		return err
	}
	if p.AlphaAlive.Min > p.AlphaAlive.Max {
		return errors.New("max EMA alpha must be greater than min EMA alpha for alive storage")
	}
	err = ensureBetween0And1(p.AlphaDead.Min, "max EMA alpha for dead storage")
	if err != nil {
		return err
	}
	err = ensureBetween0And1(p.AlphaDead.Max, "min EMA alpha for dead storage")
	if err != nil {
		return err
	}
	if p.AlphaDead.Min > p.AlphaDead.Max {
		return errors.New("max EMA alpha must be greater than min EMA alpha for dead storage")
	}
	return nil
}

// DefaultEMAParams provides the default Exponential Moving Average behavior, which is described in the unit tests.
// That is:
// (1) a fully dead storage becomes alive after 20 subsequent successful operations,
// (2) a fully alive storage becomes dead after 10 subsequent unsuccessful operations,
// (3) a storage becomes dead when it has more than 10% of unsuccessful operations (with the same weight),
// (4) a storage becomes alive when it has less than 5% of unsuccessful operations (with the same weight).
var DefaultEMAParams = EMAParams{
	AliveLimit: 0.99,
	DeadLimit:  0.88,
	AlphaAlive: EMAAlphaRange{
		Min: 0.01,
		Max: 0.05,
	},
	AlphaDead: EMAAlphaRange{
		Min: 0.1,
		Max: 0.5,
	},
}
