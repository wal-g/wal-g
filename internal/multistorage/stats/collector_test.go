package stats

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/stats/cache"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func Test_collector_AllAliveStorages(t *testing.T) {
	t.Run("takes from cache if all are relevant and any is alive", func(t *testing.T) {
		col := newTestCollector(t, 3, 1)
		setInCache(t, col, "stor_1", true, true)
		setInCache(t, col, "stor_2", false, true)
		setInCache(t, col, "stor_3", true, true)

		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		want := []string{"stor_1", "stor_3"}
		assert.Equal(t, want, alive)
	})

	t.Run("recheck outdated from cache if any", func(t *testing.T) {
		col := newTestCollector(t, 3, 1, 3)
		setInCache(t, col, "stor_1", true, true)
		setInCache(t, col, "stor_2", false, false)
		setInCache(t, col, "stor_3", true, true)

		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		want := []string{"stor_1", "stor_2", "stor_3"}
		assert.Equal(t, want, alive)
	})

	t.Run("recheck relevant from cache if all are dead", func(t *testing.T) {
		col := newTestCollector(t, 3)
		setInCache(t, col, "stor_1", false, true)
		setInCache(t, col, "stor_2", false, true)
		setInCache(t, col, "stor_3", false, true)

		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		want := []string{"stor_1", "stor_2", "stor_3"}
		assert.Equal(t, want, alive)
	})

	t.Run("recheck relevant after outdated if they are dead", func(t *testing.T) {
		col := newTestCollector(t, 3, 2)
		setInCache(t, col, "stor_1", false, true)
		setInCache(t, col, "stor_2", false, false)
		setInCache(t, col, "stor_3", false, true)

		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		want := []string{"stor_1", "stor_3"}
		assert.Equal(t, want, alive)
	})

	t.Run("recheck all if cache is empty", func(t *testing.T) {
		col := newTestCollector(t, 3, 2)
		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		want := []string{"stor_1", "stor_3"}
		assert.Equal(t, want, alive)
	})

	t.Run("provide empty slice if all are dead", func(t *testing.T) {
		col := newTestCollector(t, 1, 1)
		alive, err := col.AllAliveStorages()
		require.NoError(t, err)
		assert.Len(t, alive, 0)
	})
}

func Test_collector_FirstAliveStorage(t *testing.T) {
	t.Run("takes from cache if there is first relevant and alive", func(t *testing.T) {
		col := newTestCollector(t, 3, 2)
		setInCache(t, col, "stor_1", false, true)
		setInCache(t, col, "stor_2", true, true)
		setInCache(t, col, "stor_3", true, false)

		alive, err := col.FirstAliveStorage()
		require.NoError(t, err)
		want := "stor_2"
		assert.Equal(t, &want, alive)
	})

	t.Run("recheck outdated if there is outdated before first relevant and alive", func(t *testing.T) {
		col := newTestCollector(t, 3, 1, 2)
		setInCache(t, col, "stor_1", true, false)
		setInCache(t, col, "stor_2", true, true)
		setInCache(t, col, "stor_3", false, false)

		alive, err := col.FirstAliveStorage()
		require.NoError(t, err)
		want := "stor_2"
		assert.Equal(t, &want, alive)

		t.Run("update in cache", func(t *testing.T) {
			relevant, _, err := col.cache.Read("stor_1", "stor_3")
			require.NoError(t, err)
			assert.False(t, relevant["stor_1"])
			assert.True(t, relevant["stor_3"])
		})
	})

	t.Run("recheck relevant if there is no alive after rechecking outdated", func(t *testing.T) {
		col := newTestCollector(t, 3, 1, 3)
		setInCache(t, col, "stor_1", true, false)
		setInCache(t, col, "stor_2", false, true)
		setInCache(t, col, "stor_3", true, false)

		alive, err := col.FirstAliveStorage()
		require.NoError(t, err)
		want := "stor_2"
		assert.Equal(t, &want, alive)

		t.Run("update in cache", func(t *testing.T) {
			relevant, _, err := col.cache.Read("stor_1", "stor_2", "stor_3")
			require.NoError(t, err)
			assert.False(t, relevant["stor_1"])
			assert.True(t, relevant["stor_2"])
			assert.False(t, relevant["stor_3"])
		})
	})

	t.Run("recheck all if cache is empty", func(t *testing.T) {
		col := newTestCollector(t, 3, 1, 2)
		alive, err := col.FirstAliveStorage()
		require.NoError(t, err)
		want := "stor_3"
		assert.Equal(t, &want, alive)
	})

	t.Run("provide nil if all are dead", func(t *testing.T) {
		col := newTestCollector(t, 1, 1)
		alive, err := col.FirstAliveStorage()
		require.NoError(t, err)
		assert.Nil(t, alive)
	})
}

func Test_collector_SpecificStorage(t *testing.T) {
	t.Run("takes from cache if requested is relevant and alive", func(t *testing.T) {
		col := newTestCollector(t, 1, 1)
		setInCache(t, col, "stor_1", true, true)
		alive, err := col.SpecificStorage("stor_1")
		require.NoError(t, err)
		assert.True(t, alive)
	})

	t.Run("recheck requested if outdated in cache", func(t *testing.T) {
		col := newTestCollector(t, 2, 1)
		setInCache(t, col, "stor_1", true, false)
		setInCache(t, col, "stor_2", true, true)

		alive, err := col.SpecificStorage("stor_1")
		require.NoError(t, err)
		assert.False(t, alive)
	})

	t.Run("recheck requested if dead in cache", func(t *testing.T) {
		col := newTestCollector(t, 2)
		setInCache(t, col, "stor_1", false, true)
		setInCache(t, col, "stor_2", true, true)

		alive, err := col.SpecificStorage("stor_1")
		require.NoError(t, err)
		assert.True(t, alive)

		t.Run("update in cache", func(t *testing.T) {
			relevant, _, err := col.cache.Read("stor_1", "stor_2")
			require.NoError(t, err)
			assert.True(t, relevant["stor_1"])
		})
	})

	t.Run("recheck requested if cache is empty", func(t *testing.T) {
		col := newTestCollector(t, 2, 1)
		alive, err := col.SpecificStorage("stor_2")
		require.NoError(t, err)
		assert.True(t, alive)
	})
}

func newTestCollector(t *testing.T, storages int, deadOnCheck ...int) *collector {
	var names []string
	for i := 1; i <= storages; i++ {
		names = append(names, fmt.Sprintf("stor_%d", i))
	}
	keys := map[string]cache.Key{}
	folders := map[string]storage.Folder{}
	for _, n := range names {
		keys[n] = cache.Key{Name: n, Hash: n + "_hash"}
		folders[n] = memory.NewFolder("test/"+n, memory.NewKVS())
	}
	shMem := cache.NewSharedMemory()
	shFile := cache.NewSharedFile(path.Join(t.TempDir(), "walg_status_cache"))
	testCacheConfig := &cache.Config{TTL: time.Hour, EMAParams: &cache.DefaultEMAParams}
	testCache, err := cache.New(keys, testCacheConfig, shMem, shFile)
	require.NoError(t, err)
	check := checkMock{}
	for _, deadNum := range deadOnCheck {
		deadIdx := deadNum - 1
		name := names[deadIdx]
		folder := folders[name]
		check[folder.GetPath()] = errors.New("TEST ERROR")
	}
	checker := &AliveChecker{
		folders: folders,
		timeout: time.Hour,
		checks:  []storageCheck{check},
	}
	return NewCollector(names, testCache, checker).(*collector)
}

func setInCache(t *testing.T, col *collector, storage string, alive, relevant bool) {
	checkTime := time.Time{}
	if relevant {
		checkTime = time.Now()
	}
	_, err := col.cache.ApplyExplicitCheckResult(cache.AliveMap{storage: alive}, checkTime)
	require.NoError(t, err)
}

var _ storageCheck = checkMock{}

type checkMock map[string]error

func (cm checkMock) Check(_ context.Context, folder storage.Folder) error {
	return cm[folder.GetPath()]
}
