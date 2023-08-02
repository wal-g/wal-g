package cache

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestStatusCache(t *testing.T) {
	// Run these tests sequentially because they share common resources
	t.Run("all alive storages", testAllAliveStorages)
	t.Run("first alive storage", testFirstAliveStorage)
	t.Run("specific storage", testSpecificStorage)
}

func testAllAliveStorages(t *testing.T) {
	initTest(t)

	cache := newTestCache(2)

	t.Run("check for alive if not cached", func(_ *testing.T) {
		alive, err := cache.AllAliveStorages()
		require.NoError(t, err)
		require.Len(t, alive, 3)
		require.Equal(t, "default", alive[0].Name)
		require.Equal(t, "failover_1", alive[1].Name)
		require.Equal(t, "failover_2", alive[2].Name)
	})

	t.Run("create new cache file", func(_ *testing.T) {
		statuses, err := readFile()
		require.NoError(t, err)
		require.Len(t, statuses, 3)
	})

	updateInMem("failover_2", false)

	t.Run("take statuses from memory if relevant", func(_ *testing.T) {
		alive, err := cache.AllAliveStorages()
		require.NoError(t, err)
		require.Len(t, alive, 2)
		require.Equal(t, "default", alive[0].Name)
		require.Equal(t, "failover_1", alive[1].Name)
	})

	invalidateInMem("default")
	updateInFile(t, "failover_1", false)

	t.Run("take statuses from file if it exists and memory is outdated", func(_ *testing.T) {
		alive, err := cache.AllAliveStorages()
		require.NoError(t, err)
		require.Len(t, alive, 2)
		require.Equal(t, "default", alive[0].Name)
		require.Equal(t, "failover_2", alive[1].Name)
	})

	invalidateInMem("default")
	invalidateInFile(t, "failover_2")

	t.Run("check missing storages for alive and take others from file", func(_ *testing.T) {
		alive, err := cache.AllAliveStorages()
		require.NoError(t, err)
		require.Len(t, alive, 2)
		require.Equal(t, "default", alive[0].Name)
		require.Equal(t, "failover_2", alive[1].Name)
	})

	t.Run("update existing cache file", func(_ *testing.T) {
		statuses, err := readFile()
		require.NoError(t, err)
		require.True(t, statuses.isRelevant(time.Hour, cache.storagesInOrder[2]))
	})

	invalidateInMem("default")
	err := os.WriteFile(StatusFile, []byte("malformed content"), 0666)
	require.NoError(t, err)

	t.Run("rewrite file if malformed", func(_ *testing.T) {
		alive, err := cache.AllAliveStorages()
		require.NoError(t, err)
		require.Len(t, alive, 3)
	})
}

func testFirstAliveStorage(t *testing.T) {
	initTest(t)

	cache := newTestCache(2)

	t.Run("check for alive if not cached", func(_ *testing.T) {
		alive, err := cache.FirstAliveStorage()
		require.NoError(t, err)
		require.Equal(t, "default", alive.Name)
	})

	t.Run("create new cache file", func(_ *testing.T) {
		statuses, err := readFile()
		require.NoError(t, err)
		require.Len(t, statuses, 3)
	})

	updateInMem("default", false)

	t.Run("take statuses from memory if relevant", func(_ *testing.T) {
		alive, err := cache.FirstAliveStorage()
		require.NoError(t, err)
		require.Equal(t, "failover_1", alive.Name)
	})

	invalidateInMem("default")
	updateInFile(t, "default", false)
	updateInFile(t, "failover_1", false)

	t.Run("take statuses from file if it exists and memory is outdated", func(_ *testing.T) {
		alive, err := cache.FirstAliveStorage()
		require.NoError(t, err)
		require.Equal(t, "failover_2", alive.Name)
	})

	invalidateInMem("default")
	updateInFile(t, "failover_2", false)
	invalidateInFile(t, "failover_1")

	t.Run("check outdated storages for alive if all are dead", func(_ *testing.T) {
		alive, err := cache.FirstAliveStorage()
		require.NoError(t, err)
		require.Equal(t, "failover_1", alive.Name)
	})

	t.Run("update existing cache file", func(_ *testing.T) {
		statuses, err := readFile()
		require.NoError(t, err)
		require.True(t, statuses.isRelevant(time.Hour, cache.storagesInOrder[1]))
	})

	invalidateInMem("default")
	err := os.WriteFile(StatusFile, []byte("malformed content"), 0666)
	require.NoError(t, err)

	t.Run("rewrite file if malformed", func(_ *testing.T) {
		alive, err := cache.FirstAliveStorage()
		require.NoError(t, err)
		require.NotNil(t, alive)
	})
}

func testSpecificStorage(t *testing.T) {
	initTest(t)

	cache := newTestCache(2)

	t.Run("throws error if storage is unknown", func(_ *testing.T) {
		_, err := cache.SpecificStorage("unknown_storage")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown storage")
	})

	t.Run("check for alive if not cached", func(_ *testing.T) {
		alive, err := cache.SpecificStorage("failover_2")
		require.NoError(t, err)
		require.Equal(t, "failover_2", alive.Name)
	})

	t.Run("save to memory and cache file single status only", func(_ *testing.T) {
		require.Len(t, memCache, 1)
		statuses, err := readFile()
		require.NoError(t, err)
		require.Len(t, statuses, 1)
	})

	updateInMem("failover_2", false)

	t.Run("take statuses from memory if relevant", func(_ *testing.T) {
		alive, err := cache.SpecificStorage("failover_2")
		require.NoError(t, err)
		require.Nil(t, alive)
	})

	invalidateInMem("failover_1")
	invalidateInMem("failover_2")
	updateInFile(t, "failover_1", false)
	updateInFile(t, "failover_2", true)

	t.Run("take status from file if it exists and memory is outdated", func(_ *testing.T) {
		alive, err := cache.SpecificStorage("failover_1")
		require.NoError(t, err)
		require.Nil(t, alive)

		alive, err = cache.SpecificStorage("failover_2")
		require.NoError(t, err)
		require.Equal(t, "failover_2", alive.Name)
	})

	invalidateInMem("failover_1")
	invalidateInFile(t, "failover_1")

	t.Run("check storage for alive if it is outdated in memory and file", func(_ *testing.T) {
		alive, err := cache.SpecificStorage("failover_1")
		require.NoError(t, err)
		require.Equal(t, "failover_1", alive.Name)
	})

	t.Run("update existing cache file", func(_ *testing.T) {
		statuses, err := readFile()
		require.NoError(t, err)
		require.Len(t, statuses, 2)
		require.True(t, statuses.isRelevant(time.Hour, cache.storagesInOrder[1]))
	})

	invalidateInMem("default")
	err := os.WriteFile(StatusFile, []byte("malformed content"), 0666)
	require.NoError(t, err)

	t.Run("rewrite file if malformed", func(_ *testing.T) {
		alive, err := cache.SpecificStorage("default")
		require.NoError(t, err)
		require.NotNil(t, alive)
	})
}

func initTest(t *testing.T) {
	t.Cleanup(func() {
		_ = os.Remove(StatusFile)
	})
	memCache = storageStatuses{}
}

func invalidateInMem(storage string) {
	memCache[storage] = status{
		Alive:   true,
		Checked: time.Unix(0, 0),
	}
}

func updateInMem(storage string, alive bool) {
	memCache[storage] = status{
		Alive:   alive,
		Checked: time.Now(),
	}
}

func invalidateInFile(t *testing.T, storage string) {
	statuses, err := readFile()
	require.NoError(t, err)
	statuses[storage] = status{
		Alive:   true,
		Checked: time.Unix(0, 0),
	}
	err = writeFile(statuses)
	require.NoError(t, err)
}

func updateInFile(t *testing.T, storage string, alive bool) {
	statuses, err := readFile()
	require.NoError(t, err)
	statuses[storage] = status{
		Alive:   alive,
		Checked: time.Now(),
	}
	err = writeFile(statuses)
	require.NoError(t, err)
}

func newTestCache(failoverStorages int) *statusCache {
	failover := map[string]storage.Folder{}
	for i := 0; i < failoverStorages; i++ {
		failover[fmt.Sprintf("failover_%d", i+1)] = memory.NewFolder("", memory.NewStorage())
	}
	return NewStatusCache(
		memory.NewFolder("", memory.NewStorage()),
		failover,
		time.Hour,
		time.Hour,
	).(*statusCache)
}
