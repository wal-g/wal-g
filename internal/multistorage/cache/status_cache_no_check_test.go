package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestStatusCacheNoCheck(t *testing.T) {
	failover := map[string]storage.Folder{}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("failover_%d", i+1)
		failover[name] = memory.NewFolder(name+"/", memory.NewStorage())
	}
	primary := memory.NewFolder("default/", memory.NewStorage())
	c, err := NewStatusCacheNoCheck(
		primary,
		failover,
	)
	require.NoError(t, err)

	t.Run("AllAliveStorages simply returns all storages without checking", func(t *testing.T) {
		got, err := c.AllAliveStorages()
		assert.NoError(t, err)
		assert.Equal(t, "default", got[0].Name)
		assert.Equal(t, primary, got[0].Folder)
		for i := 1; i <= 3; i++ {
			name := fmt.Sprintf("failover_%d", i)
			assert.Equal(t, name, got[i].Name)
			assert.Equal(t, failover[name], got[i].Folder)
		}
	})

	t.Run("AllAliveStorages returns default storage", func(t *testing.T) {
		got, err := c.FirstAliveStorage()
		assert.NoError(t, err)
		assert.Equal(t, "default", got.Name)
		assert.Equal(t, primary, got.Folder)
	})

	t.Run("SpecificStorage returns storage with requested name", func(t *testing.T) {
		got, err := c.SpecificStorage("default")
		assert.NoError(t, err)
		assert.Equal(t, "default", got.Name)
		assert.Equal(t, primary, got.Folder)

		got, err = c.SpecificStorage("failover_2")
		assert.NoError(t, err)
		assert.Equal(t, "failover_2", got.Name)
		assert.Equal(t, failover["failover_2"], got.Folder)
	})
}
