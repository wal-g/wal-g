package cache

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_cache_Read(t *testing.T) {
	t.Run("take from mem if cached and relevant", func(t *testing.T) {
		c := newTestCache(t, 1, false)
		c.shMem.Statuses[key("def")] = status(true, true)
		c.shMem.Statuses[key("fo1")] = status(false, true)

		relevant, outdated, err := c.Read("def", "fo1")
		require.NoError(t, err)

		wantRelevant := AliveMap{
			"def": true,
			"fo1": false,
		}
		assert.Equal(t, wantRelevant, relevant)
		assert.Empty(t, outdated)
	})

	t.Run("merge with file any in mem is outdated", func(t *testing.T) {
		c := newTestCache(t, 2, true)
		c.shMem.Statuses[key("def")] = status(true, false)
		c.shMem.Statuses[key("fo1")] = status(false, true)
		c.shMem.Statuses[key("fo2")] = status(false, false)
		time.Sleep(time.Millisecond)
		err := c.shFile.write(storageStatuses{
			key("def"): status(false, true),
			key("fo1"): status(true, true),
		})
		require.NoError(t, err)

		relevant, outdated, err := c.Read("def", "fo1", "fo2")
		require.NoError(t, err)

		wantRelevant := AliveMap{
			"def": false,
			"fo1": true,
		}
		wantOutdated := AliveMap{
			"fo2": false,
		}
		assert.Equal(t, wantRelevant, relevant)
		assert.Equal(t, wantOutdated, outdated)
	})

	t.Run("take from mem when it is outdated but file is not used", func(t *testing.T) {
		c := newTestCache(t, 1, false)
		c.shMem.Statuses[key("def")] = status(true, true)
		c.shMem.Statuses[key("fo1")] = status(false, false)

		relevant, outdated, err := c.Read("def", "fo1")
		require.NoError(t, err)

		wantRelevant := AliveMap{
			"def": true,
		}
		wantOutdated := AliveMap{
			"fo1": false,
		}
		assert.Equal(t, wantRelevant, relevant)
		assert.Equal(t, wantOutdated, outdated)
	})

	t.Run("take from file when mem is empty", func(t *testing.T) {
		c := newTestCache(t, 2, false)
		c.shMem.Statuses[key("def")] = status(true, false)
		c.shMem.Statuses[key("fo1")] = status(false, true)
		c.shMem.Statuses[key("fo2")] = status(false, false)

		relevant, outdated, err := c.Read("def", "fo1", "fo2")
		require.NoError(t, err)

		wantRelevant := AliveMap{
			"fo1": false,
		}
		wantOutdated := AliveMap{
			"def": true,
			"fo2": false,
		}
		assert.Equal(t, wantRelevant, relevant)
		assert.Equal(t, wantOutdated, outdated)
	})

	t.Run("consider all storages outdated when mem and file are empty", func(t *testing.T) {
		c := newTestCache(t, 2, false)

		relevant, outdated, err := c.Read("def", "fo1", "fo2")
		require.NoError(t, err)

		assert.Empty(t, relevant)
		assert.Len(t, outdated, 3)
	})

	t.Run("don't provide statuses for not requested keys", func(t *testing.T) {
		t.Run("mem only", func(t *testing.T) {
			c := newTestCache(t, 1, false)
			c.shMem.Statuses[key("def")] = status(true, true)
			c.shMem.Statuses[key("fo1")] = status(true, true)

			relevant, outdated, err := c.Read("def")
			require.NoError(t, err)

			assert.ElementsMatch(t, []string{"def"}, relevant.Names())
			assert.Empty(t, outdated)
		})

		t.Run("merged mem and file", func(t *testing.T) {
			c := newTestCache(t, 3, true)
			c.shMem.Statuses[key("def")] = status(true, true)
			c.shMem.Statuses[key("fo1")] = status(true, false)
			c.shMem.Statuses[key("fo3")] = status(false, true)
			err := c.shFile.write(storageStatuses{
				key("def"): status(false, true),
				key("fo2"): status(true, false),
				key("fo3"): status(true, true),
			})
			require.NoError(t, err)

			relevant, outdated, err := c.Read("def", "fo1", "fo2")
			require.NoError(t, err)

			assert.ElementsMatch(t, []string{"def"}, relevant.Names())
			assert.ElementsMatch(t, []string{"fo1", "fo2"}, outdated.Names())
		})

		t.Run("file only", func(t *testing.T) {
			c := newTestCache(t, 2, true)
			err := c.shFile.write(storageStatuses{
				key("def"): status(false, true),
				key("fo1"): status(true, false),
				key("fo2"): status(true, true),
			})
			require.NoError(t, err)

			relevant, outdated, err := c.Read("def", "fo1")
			require.NoError(t, err)

			assert.ElementsMatch(t, []string{"def"}, relevant.Names())
			assert.ElementsMatch(t, []string{"fo1"}, outdated.Names())
		})
	})
}

func Test_cache_ApplyExplicitCheckResult(t *testing.T) {
	t.Run("applies check result to mem", func(t *testing.T) {
		c := newTestCache(t, 2, false)
		WithCustomFlushTimeout(time.Hour)(c)
		checkRes := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		_, err := c.ApplyExplicitCheckResult(checkRes, time.Now())
		require.NoError(t, err)
		assert.Equal(t, checkRes, c.shMem.Statuses.aliveMap(c.emaParams))
	})

	t.Run("applies check result to file if flush timeout exceeded", func(t *testing.T) {
		c := newTestCache(t, 2, true)
		WithCustomFlushTimeout(0)(c)
		checkRes := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		_, err := c.ApplyExplicitCheckResult(checkRes, time.Now())
		require.NoError(t, err)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.Equal(t, checkRes, fileStatuses.aliveMap(c.emaParams))
	})

	t.Run("applies check result to file if file does not exist", func(t *testing.T) {
		c := newTestCache(t, 2, true)
		WithCustomFlushTimeout(time.Hour)(c)
		checkRes := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		_, err := c.ApplyExplicitCheckResult(checkRes, time.Now())
		require.NoError(t, err)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.Equal(t, checkRes, fileStatuses.aliveMap(c.emaParams))
	})

	t.Run("does not apply check result to file if flush timeout did not exceed", func(t *testing.T) {
		c := newTestCache(t, 2, true)
		c.shFile.Updated = time.Now()
		WithCustomFlushTimeout(time.Hour)(c)

		err := c.shFile.write(nil)
		require.NoError(t, err)

		checkRes := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		_, err = c.ApplyExplicitCheckResult(checkRes, time.Now())
		require.NoError(t, err)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.Len(t, fileStatuses.aliveMap(c.emaParams), 0)
	})

	t.Run("merges old mem and file statuses with new check result", func(t *testing.T) {
		c := newTestCache(t, 3, true)
		WithCustomFlushTimeout(0)(c)

		c.shMem.Statuses = storageStatuses{
			key("def"): status(false, false),
			key("fo1"): status(false, true),
			key("fo2"): status(true, false),
		}
		err := c.shFile.write(storageStatuses{
			key("def"): status(true, true),
			key("fo1"): status(true, false),
			key("fo2"): status(false, true),
			key("fo3"): status(false, false),
		})
		require.NoError(t, err)

		checkRes := AliveMap{
			"def": true,
			"fo2": true,
		}
		_, err = c.ApplyExplicitCheckResult(checkRes, time.Now())
		require.NoError(t, err)

		wantMem := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		assert.Equal(t, wantMem, c.shMem.Statuses.aliveMap(c.emaParams))

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		wantFile := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
			"fo3": false,
		}
		assert.Equal(t, wantFile, fileStatuses.aliveMap(c.emaParams))
	})

	t.Run("provides actual aliveness for requested storages", func(t *testing.T) {
		c := newTestCache(t, 2, false)
		c.shMem.Statuses = storageStatuses{
			key("def"): status(false, false),
			key("fo1"): status(true, true),
			key("fo2"): status(false, false),
		}
		checkRes := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
		}
		got, err := c.ApplyExplicitCheckResult(checkRes, time.Now(), "def", "fo1")
		require.NoError(t, err)

		want := AliveMap{
			"def": true,
			"fo1": false,
		}
		assert.Equal(t, want, got)
	})
}

func Test_cache_ApplyOperationResult(t *testing.T) {
	t.Run("applies operation result to mem", func(t *testing.T) {
		c := newTestCache(t, 0, false)
		WithCustomFlushTimeout(time.Hour)(c)
		c.ApplyOperationResult("def", true, 100)
		assert.True(t, c.shMem.Statuses[key("def")].alive(c.emaParams))
	})

	t.Run("applies operation result to file if flush timeout exceeded", func(t *testing.T) {
		c := newTestCache(t, 1, true)
		WithCustomFlushTimeout(0)(c)

		c.ApplyOperationResult("fo1", true, 100)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.True(t, fileStatuses[key("fo1")].alive(c.emaParams))
	})

	t.Run("apply operation result to file if file does not exist", func(t *testing.T) {
		c := newTestCache(t, 1, true)
		WithCustomFlushTimeout(time.Hour)(c)

		c.ApplyOperationResult("fo1", true, 100)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.True(t, fileStatuses[key("fo1")].alive(c.emaParams))
	})

	t.Run("does not apply operation result to file if flush timeout did not exceed", func(t *testing.T) {
		c := newTestCache(t, 1, true)
		c.shFile.Updated = time.Now()
		WithCustomFlushTimeout(time.Hour)(c)
		err := c.shFile.write(nil)
		require.NoError(t, err)

		c.ApplyOperationResult("fo1", true, 100)

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.NotContains(t, fileStatuses, key("fo1"))
	})

	t.Run("does not apply operation result if storage name is not used", func(t *testing.T) {
		c := newTestCache(t, 1, true)
		WithCustomFlushTimeout(0)(c)
		err := c.shFile.write(nil)
		require.NoError(t, err)

		c.ApplyOperationResult("fo2", true, 100)

		assert.NotContains(t, c.shMem.Statuses, key("fo2"))

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		assert.NotContains(t, fileStatuses, key("fo2"))
	})

	t.Run("merges old mem and file statuses with new operation result", func(t *testing.T) {
		c := newTestCache(t, 3, true)
		WithCustomFlushTimeout(0)(c)
		c.shMem.Statuses = storageStatuses{
			key("def"): status(false, false),
			key("fo1"): status(false, true),
			key("fo2"): status(false, true),
		}
		err := c.shFile.write(storageStatuses{
			key("def"): status(true, true),
			key("fo1"): status(true, false),
			key("fo2"): status(false, false),
			key("fo3"): status(true, false),
		})
		require.NoError(t, err)

		c.ApplyOperationResult("fo2", false, 100)

		wantMem := AliveMap{
			"def": false,
			"fo1": false,
			"fo2": false,
		}
		assert.Equal(t, wantMem, c.shMem.Statuses.aliveMap(c.emaParams))

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		wantFile := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": false,
			"fo3": true,
		}
		assert.Equal(t, wantFile, fileStatuses.aliveMap(c.emaParams))
	})
}

func Test_cache_Flush(t *testing.T) {
	t.Run("works with nil file", func(t *testing.T) {
		c := newTestCache(t, 0, false)
		c.shMem.Statuses = storageStatuses{
			key("def"): status(true, true),
		}
		c.Flush()
		isLocked := !c.shMem.TryLock()
		defer c.shMem.Unlock()
		assert.False(t, isLocked)
	})

	t.Run("creates empty file if mem is empty", func(t *testing.T) {
		c := newTestCache(t, 0, true)
		c.shMem.Statuses = storageStatuses{}
		c.Flush()
		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)
		assert.Empty(t, fileStatuses)
	})

	t.Run("merges existing file content with mem", func(t *testing.T) {
		c := newTestCache(t, 3, true)
		c.shMem.Statuses = storageStatuses{
			key("def"): status(true, true),
			key("fo1"): status(true, false),
			key("fo2"): status(true, false),
		}
		err := c.shFile.write(storageStatuses{
			key("def"): status(false, false),
			key("fo1"): status(false, true),
			key("fo3"): status(false, false),
		})
		require.NoError(t, err)

		c.Flush()

		fileStatuses, err := c.shFile.read()
		require.NoError(t, err)

		want := AliveMap{
			"def": true,
			"fo1": false,
			"fo2": true,
			"fo3": false,
		}
		assert.Equal(t, want, fileStatuses.aliveMap(c.emaParams))
	})
}

func Test_cache_correspondingKeys(t *testing.T) {
	t.Run("matches names with used keys", func(t *testing.T) {
		c := newTestCache(t, 2, false)
		got, err := c.correspondingKeys("def", "fo1", "fo2")
		require.NoError(t, err)
		want := []Key{key("def"), key("fo1"), key("fo2")}
		assert.Equal(t, want, got)
	})

	t.Run("throw err when key is not used", func(t *testing.T) {
		c := newTestCache(t, 1, false)
		got, err := c.correspondingKeys("def", "fo1", "fo2")
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("works with no names", func(t *testing.T) {
		c := newTestCache(t, 1, false)
		got, err := c.correspondingKeys()
		require.NoError(t, err)
		require.Len(t, got, 0)
	})
}

func newTestCache(t *testing.T, failoverStorages int, useFile bool) *cache {
	keysMap := map[string]Key{
		"def": key("def"),
	}
	for i := 0; i < failoverStorages; i++ {
		name := fmt.Sprintf("fo%d", i+1)
		keysMap[name] = key(name)
	}
	shMem := NewSharedMemory()
	var shFile *SharedFile
	if useFile {
		shFile = NewSharedFile(path.Join(t.TempDir(), "walg_status_cache"))
	}
	c, err := New(
		keysMap,
		&Config{TTL: time.Hour, EMAParams: &DefaultEMAParams},
		shMem,
		shFile,
	)
	require.NoError(t, err)
	return c.(*cache)
}

func key(name string) Key {
	return Key{Name: name, Hash: name + "_hash"}
}
