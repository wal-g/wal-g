package stats

import (
	"fmt"
	"time"

	"github.com/wal-g/wal-g/internal/multistorage/stats/cache"
)

// Collector collects information about the success of operations performed with some storages, and answers which
// storages are considered alive or dead at the moment, based on the time-aggregated statistics.
//
//go:generate mockgen -source collector.go -destination collector_mock.go -package stats
type Collector interface {
	AllAliveStorages() ([]string, error)
	FirstAliveStorage() (*string, error)
	SpecificStorage(name string) (bool, error)
	ReportOperationResult(storage string, op OperationWeight, success bool)
	Close() error
}

var _ Collector = &collector{}

type collector struct {
	storagesInOrder []string
	cache           cache.Cache
	aliveChecker    *AliveChecker
}

func NewCollector(storagesInOrder []string, cache cache.Cache, aliveChecker *AliveChecker) Collector {
	return &collector{
		storagesInOrder: storagesInOrder,
		cache:           cache,
		aliveChecker:    aliveChecker,
	}
}

func (c *collector) AllAliveStorages() ([]string, error) {
	relevant, outdated, err := c.cache.Read(c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("read status cache: %w", err)
	}
	allRelevant := len(outdated) == 0
	if allRelevant {
		alive := relevant.AliveNames(c.storagesInOrder)
		if len(alive) > 0 {
			return alive, nil
		}
	}

	outdatedCheckResult := c.aliveChecker.CheckForAlive(outdated.Names()...)
	afterRecheckOutdated, err := c.cache.ApplyExplicitCheckResult(outdatedCheckResult, time.Now(), c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("apply outdated storages check result: %w", err)
	}
	if alive := afterRecheckOutdated.AliveNames(c.storagesInOrder); len(alive) > 0 {
		return alive, nil
	}

	relevantCheckResult := c.aliveChecker.CheckForAlive(relevant.Names()...)
	afterRecheckAll, err := c.cache.ApplyExplicitCheckResult(relevantCheckResult, time.Now(), c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("apply relevant storages check result: %w", err)
	}
	return afterRecheckAll.AliveNames(c.storagesInOrder), nil
}

func (c *collector) FirstAliveStorage() (*string, error) {
	relevant, outdated, err := c.cache.Read(c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("read status cache: %w", err)
	}
	firstRelevantAndAlive := relevant.FirstAlive(c.storagesInOrder)
	if firstRelevantAndAlive != nil {
		return firstRelevantAndAlive, nil
	}

	outdatedCheckResult := c.aliveChecker.CheckForAlive(outdated.Names()...)
	afterRecheckOutdated, err := c.cache.ApplyExplicitCheckResult(outdatedCheckResult, time.Now(), c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("apply outdated storages check result: %w", err)
	}
	firstRelevantAndAlive = afterRecheckOutdated.FirstAlive(c.storagesInOrder)
	if firstRelevantAndAlive != nil {
		return firstRelevantAndAlive, nil
	}

	relevantCheckResult := c.aliveChecker.CheckForAlive(relevant.Names()...)
	afterRecheckAll, err := c.cache.ApplyExplicitCheckResult(relevantCheckResult, time.Now(), c.storagesInOrder...)
	if err != nil {
		return nil, fmt.Errorf("apply relevant storages check result: %w", err)
	}
	return afterRecheckAll.FirstAlive(c.storagesInOrder), nil
}

func (c *collector) SpecificStorage(name string) (bool, error) {
	relevant, _, err := c.cache.Read(name)
	if err != nil {
		return false, fmt.Errorf("read status cache: %w", err)
	}
	if alive, ok := relevant[name]; ok && alive {
		return true, nil
	}

	checkResult := c.aliveChecker.CheckForAlive(name)
	afterRecheck, err := c.cache.ApplyExplicitCheckResult(checkResult, time.Now(), name)
	if err != nil {
		return false, fmt.Errorf("apply storage %q check result: %w", name, err)
	}

	return afterRecheck[name], nil
}

func (c *collector) ReportOperationResult(storage string, opWeight OperationWeight, success bool) {
	c.cache.ApplyOperationResult(storage, success, float64(opWeight))
}

func (c *collector) Close() error {
	c.cache.Flush()
	return nil
}
