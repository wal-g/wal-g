package cache

import (
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var (
	outdatedTime = time.UnixMilli(0)
	relevantTime = time.Now()
)

func TestStatusCache_AllAliveStorages(t *testing.T) {
	t.Run("take from mem if cached and relevant", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", true, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if any is outdated", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", true, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, outdatedTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1", "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if all are dead", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1", "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if it is empty", func(t *testing.T) {
		cache := testCache(t, 2, false)
		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1", "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("take from file if any in mem is outdated", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", true, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, outdatedTime)

		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "failover_1")
		assert.Equal(t, want, got)
	})

	t.Run("take from file if all in mem are dead", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", false, relevantTime)

		setInFile(t, cache, "default", true, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from file if all are dead", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", false, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1", "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("recheck storages outdated in file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", true, relevantTime)
		setInFile(t, cache, "failover_1", false, outdatedTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1")
		assert.Equal(t, want, got)
	})

	t.Run("update mem after reading from file", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", false, relevantTime)

		setInFile(t, cache, "default", true, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		_, err := cache.AllAliveStorages()
		require.NoError(t, err)

		file, err := readFile(cache.sharedFilePath)
		require.NoError(t, err)
		assert.Equal(t, file, *cache.sharedMem)
	})

	t.Run("update mem and file after checking", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", false, outdatedTime)
		setInMem(t, cache, "failover_1", false, outdatedTime)
		setInMem(t, cache, "failover_2", false, outdatedTime)

		setInFile(t, cache, "default", false, outdatedTime)
		setInFile(t, cache, "failover_1", false, outdatedTime)
		setInFile(t, cache, "failover_2", false, outdatedTime)

		_, err := cache.AllAliveStorages()
		require.NoError(t, err)

		for _, st := range cache.storagesInOrder {
			status := getFromMem(t, cache, st.Name)
			assert.True(t, status.Alive)
			assert.NotEqual(t, int64(0), status.Checked.UnixMilli())

			status = getFromFile(t, cache, st.Name)
			assert.True(t, status.Alive)
			assert.NotEqual(t, int64(0), status.Checked.UnixMilli())
		}
	})

	t.Run("do not fail if cannot read or write file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.sharedFilePath = path.Join(t.TempDir(), "non-accessible-file")
		_, err := os.Create(cache.sharedFilePath)
		require.NoError(t, err)
		err = os.Chmod(cache.sharedFilePath, 0000)
		require.NoError(t, err)

		_, err = cache.AllAliveStorages()
		require.NoError(t, err)
	})

	t.Run("recheck all storages if ttl is zero", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.ttl = 0

		setInMem(t, cache, "default", true, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.AllAliveStorages()
		require.NoError(t, err)

		want := aliveFolders(cache, "default", "failover_1", "failover_2")
		assert.Equal(t, want, got)
	})
}

func TestStatusCache_FirstAliveStorage(t *testing.T) {
	t.Run("take from mem first relevant and alive", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_2")
		assert.Equal(t, want, got)
	})

	t.Run("do not take alive from mem if any previous is outdated", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", false, outdatedTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if all are dead", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", false, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if it is empty", func(t *testing.T) {
		cache := testCache(t, 2, false)
		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("take from file if no relevant first alive in mem", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", true, outdatedTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_1")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from file if no relevant first alive", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", false, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("recheck storages outdated in file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", true, outdatedTime)
		setInFile(t, cache, "failover_1", false, outdatedTime)
		setInFile(t, cache, "failover_2", true, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("update first alive in mem after reading from file", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", false, relevantTime)
		setInMem(t, cache, "failover_2", false, relevantTime)

		setInFile(t, cache, "default", true, relevantTime)
		setInFile(t, cache, "failover_1", true, relevantTime)
		setInFile(t, cache, "failover_2", false, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		assert.Equal(t, "default", got.Name)
		status := getFromMem(t, cache, "default")
		assert.True(t, status.Alive)
		assert.Equal(t, relevantTime.UnixMilli(), status.Checked.UnixMilli())
	})

	t.Run("update mem and file after checking", func(t *testing.T) {
		cache := testCache(t, 2, true)

		setInMem(t, cache, "default", false, outdatedTime)
		setInMem(t, cache, "failover_1", false, outdatedTime)
		setInMem(t, cache, "failover_2", false, outdatedTime)

		setInFile(t, cache, "default", false, outdatedTime)
		setInFile(t, cache, "failover_1", false, outdatedTime)
		setInFile(t, cache, "failover_2", false, outdatedTime)

		_, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		for _, st := range cache.storagesInOrder {
			status := getFromMem(t, cache, st.Name)
			assert.True(t, status.Alive)
			assert.NotEqual(t, int64(0), status.Checked.UnixMilli())

			status = getFromFile(t, cache, st.Name)
			assert.True(t, status.Alive)
			assert.NotEqual(t, int64(0), status.Checked.UnixMilli())
		}
	})

	t.Run("do not fail if cannot read or write file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.sharedFilePath = path.Join(t.TempDir(), "non-accessible-file")
		_, err := os.Create(cache.sharedFilePath)
		require.NoError(t, err)
		err = os.Chmod(cache.sharedFilePath, 0000)
		require.NoError(t, err)

		_, err = cache.FirstAliveStorage()
		require.NoError(t, err)
	})

	t.Run("recheck all storages if ttl is zero", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.ttl = 0

		setInMem(t, cache, "default", false, relevantTime)
		setInMem(t, cache, "failover_1", true, relevantTime)
		setInMem(t, cache, "failover_2", true, relevantTime)

		setInFile(t, cache, "default", false, relevantTime)
		setInFile(t, cache, "failover_1", false, relevantTime)
		setInFile(t, cache, "failover_2", true, relevantTime)

		got, err := cache.FirstAliveStorage()
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})
}

func TestStatusCache_SpecificStorage(t *testing.T) {
	t.Run("take from mem if relevant and alive", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "failover_1", true, relevantTime)

		got, err := cache.SpecificStorage("failover_1")
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_1")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if outdated", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "failover_2", true, outdatedTime)

		got, err := cache.SpecificStorage("failover_2")
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_2")
		assert.Equal(t, want, got)
		assertWasChecked(t, cache, "failover_2")
	})

	t.Run("do not take from mem if dead", func(t *testing.T) {
		cache := testCache(t, 2, false)
		setInMem(t, cache, "default", false, relevantTime)

		got, err := cache.SpecificStorage("default")
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
	})

	t.Run("do not take from mem if it is empty", func(t *testing.T) {
		cache := testCache(t, 2, false)
		got, err := cache.SpecificStorage("failover_1")
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_1")
		assert.Equal(t, want, got)
		assertWasChecked(t, cache, "failover_1")
	})

	t.Run("take from file if it is alive and not in mem", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "failover_1", true, relevantTime)

		got, err := cache.SpecificStorage("failover_1")
		require.NoError(t, err)

		want := aliveFolder(cache, "failover_1")
		assert.Equal(t, want, got)
		assertWasNotChecked(t, cache, "failover_1")
	})

	t.Run("do not take from file if it is outdated", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", true, outdatedTime)

		got, err := cache.SpecificStorage("default")
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
		assertWasChecked(t, cache, "default")
	})

	t.Run("do not take from file if it is dead", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInFile(t, cache, "default", false, relevantTime)

		got, err := cache.SpecificStorage("default")
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
		assertWasChecked(t, cache, "default")
	})

	t.Run("update specific in mem after reading from file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInMem(t, cache, "default", false, outdatedTime)
		setInFile(t, cache, "default", true, relevantTime)

		got, err := cache.SpecificStorage("default")
		require.NoError(t, err)

		assert.Equal(t, "default", got.Name)
		status := getFromMem(t, cache, "default")
		assert.True(t, status.Alive)
		assert.Equal(t, relevantTime.UnixMilli(), status.Checked.UnixMilli())
	})

	t.Run("update mem and file after checking", func(t *testing.T) {
		cache := testCache(t, 2, true)
		setInMem(t, cache, "failover_2", false, outdatedTime)
		setInFile(t, cache, "failover_2", false, outdatedTime)

		_, err := cache.SpecificStorage("failover_2")
		require.NoError(t, err)

		status := getFromMem(t, cache, "failover_2")
		assert.True(t, status.Alive)
		assert.NotEqual(t, int64(0), status.Checked.UnixMilli())

		status = getFromFile(t, cache, "failover_2")
		assert.True(t, status.Alive)
		assert.NotEqual(t, int64(0), status.Checked.UnixMilli())
	})

	t.Run("do not fail if cannot read or write file", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.sharedFilePath = path.Join(t.TempDir(), "non-accessible-file")
		_, err := os.Create(cache.sharedFilePath)
		require.NoError(t, err)
		err = os.Chmod(cache.sharedFilePath, 0000)
		require.NoError(t, err)

		_, err = cache.SpecificStorage("default")
		require.NoError(t, err)
	})

	t.Run("recheck specific storage if ttl is zero", func(t *testing.T) {
		cache := testCache(t, 2, true)
		cache.ttl = 0

		setInMem(t, cache, "default", true, relevantTime)
		setInFile(t, cache, "default", true, relevantTime)

		got, err := cache.SpecificStorage("default")
		require.NoError(t, err)

		want := aliveFolder(cache, "default")
		assert.Equal(t, want, got)
		assertWasChecked(t, cache, "default")
	})

	t.Run("throw err if storage is unknown", func(t *testing.T) {
		cache := testCache(t, 2, true)

		_, err := cache.SpecificStorage("failover_3")
		require.Error(t, err)
	})
}

func testCache(t *testing.T, failoverStorages int, useFile bool) *statusCache {
	failover := map[string]storage.Folder{}
	for i := 0; i < failoverStorages; i++ {
		name := fmt.Sprintf("failover_%d", i+1)
		failover[name] = memory.NewFolder(name+"/", memory.NewStorage())
	}
	fileOpt := WithoutSharedFile()
	if useFile {
		fileOpt = WithSharedFile(path.Join(t.TempDir(), "walg_status_cache"))
	}
	c, err := NewStatusCache(
		memory.NewFolder("default/", memory.NewStorage()),
		failover,
		time.Hour,
		NewRWAliveChecker(time.Hour, 1024),
		WithSharedMemory(&storageStatuses{}, new(sync.Mutex)),
		fileOpt,
	)
	require.NoError(t, err)
	return c.(*statusCache)
}

func setInMem(t *testing.T, cache *statusCache, storage string, alive bool, checked time.Time) {
	key := storageKey(t, cache, storage)
	(*cache.sharedMem)[key] = status{alive, checked}
}

func getFromMem(t *testing.T, cache *statusCache, storage string) status {
	key := storageKey(t, cache, storage)
	return (*cache.sharedMem)[key]
}

func setInFile(t *testing.T, cache *statusCache, storage string, alive bool, checked time.Time) {
	file, _ := readFile(cache.sharedFilePath)
	k := storageKey(t, cache, storage)
	if file == nil {
		file = map[key]status{}
	}
	file[k] = status{alive, checked}
	err := writeFile(cache.sharedFilePath, file)
	require.NoError(t, err)
}

func getFromFile(t *testing.T, cache *statusCache, storage string) status {
	file, err := readFile(cache.sharedFilePath)
	require.NoError(t, err)
	k := storageKey(t, cache, storage)
	s, ok := file[k]
	if !ok {
		t.Fatalf("no storage %q in file cache", storage)
	}
	return s
}

func storageKey(t *testing.T, cache *statusCache, storage string) key {
	for _, s := range cache.storagesInOrder {
		if s.Name == storage {
			return s.Key
		}
	}
	t.Fatalf("unknown storage %q", storage)
	return ""
}

func assertWasChecked(t *testing.T, cache *statusCache, storage string) {
	assert.True(t, getFromMem(t, cache, storage).Checked.After(relevantTime))
}

func assertWasNotChecked(t *testing.T, cache *statusCache, storage string) {
	assert.True(t, getFromMem(t, cache, storage).Checked.Before(relevantTime.Add(time.Duration(1))))
}

func aliveFolders(cache *statusCache, names ...string) []NamedFolder {
	namesMap := make(map[string]bool, len(names))
	for _, n := range names {
		namesMap[n] = true
	}
	res := make([]NamedFolder, 0, len(names))
	for _, s := range cache.storagesInOrder {
		if namesMap[s.Name] {
			res = append(res, s)
		}
	}
	return res
}

func aliveFolder(cache *statusCache, name string) *NamedFolder {
	for _, s := range cache.storagesInOrder {
		if s.Name == name {
			return &s
		}
	}
	return nil
}
