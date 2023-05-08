package multistorage

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/wal-g/tracelog"
)

var storageCacheMu = &sync.RWMutex{}

type aliveStorageCache struct {
	LastGoodStorage string        `json:"last_ok_storage"`
	UpdTS           time.Time     `json:"upd_ts"`
	Found           bool          `json:"-"`
	Lifetime        time.Duration `json:"-"`
}

const cacheFileName = ".walg_failover_storage_cache"

var storageCache *aliveStorageCache

func initStorageCache() error {
	cacheLifetime, err := internal.GetDurationSetting(internal.PgFailoverStorageCacheLifetime)
	if err != nil {
		return fmt.Errorf("cache lifetime setting: %v", err)
	}

	if storageCache == nil {
		storageCacheMu.Lock()
		defer storageCacheMu.Unlock()
		if storageCache == nil {
			cache, err := readState()
			if err != nil {
				tracelog.DebugLogger.Printf("Reading storage cache: %v", err)
				// ignore the error and continue
			} else {
				cache.Found = true
			}
			cache.Lifetime = cacheLifetime
			storageCache = &cache
		}
	}
	return nil
}

func (c *aliveStorageCache) IsActual() bool {
	storageCacheMu.RLock()
	defer storageCacheMu.RUnlock()

	return c.Found && time.Now().Before(c.UpdTS.Add(c.Lifetime))
}

func (c *aliveStorageCache) Copy() aliveStorageCache {
	storageCacheMu.RLock()
	defer storageCacheMu.RUnlock()

	return *c
}

func (c *aliveStorageCache) Update(storage string) {
	if storageCache.IsActual() {
		// too early to update
		return
	}
	storageCacheMu.Lock()
	defer storageCacheMu.Unlock()

	storageCache.LastGoodStorage = storage
	storageCache.UpdTS = time.Now()

	err := storageCache.writeState()
	if err != nil {
		tracelog.DebugLogger.Printf("Writing storage cache: %v", err)
	}
}

func (c *aliveStorageCache) writeState() error {
	usr, err := user.Current()

	if err != nil {
		return err
	}

	tracelog.DebugLogger.Printf("Writing storage cache: %v", c)
	cacheFilename := filepath.Join(usr.HomeDir, cacheFileName)
	marshal, err := json.Marshal(c)
	if err != nil {
		return err
	}

	return os.WriteFile(cacheFilename, marshal, 0644)
}

func readState() (aliveStorageCache, error) {
	var cache aliveStorageCache

	usr, err := user.Current()
	if err != nil {
		return aliveStorageCache{}, err
	}

	cacheFilename := filepath.Join(usr.HomeDir, cacheFileName)
	file, err := os.ReadFile(cacheFilename)
	if err != nil {
		return aliveStorageCache{}, err
	}

	err = json.Unmarshal(file, &cache)
	if err != nil {
		return aliveStorageCache{}, err
	}

	tracelog.DebugLogger.Printf("Reading storage cache: %v", cache)
	return cache, nil
}
