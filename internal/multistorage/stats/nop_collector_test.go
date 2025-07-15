package stats

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_nopCollector(t *testing.T) {
	c := NewNopCollector(
		[]string{
			"default",
			"failover_1",
			"failover_2",
			"failover_3",
		},
	)

	t.Run("AllAliveStorages simply returns all storages without checking", func(t *testing.T) {
		got, err := c.AllAliveStorages()
		assert.NoError(t, err)
		assert.Equal(t, "default", got[0])
		for i := 1; i <= 3; i++ {
			name := fmt.Sprintf("failover_%d", i)
			assert.Equal(t, name, got[i])
		}
	})

	t.Run("FirstAliveStorage returns default storage", func(t *testing.T) {
		got, err := c.FirstAliveStorage()
		assert.NoError(t, err)
		assert.Equal(t, "default", *got)
	})

	t.Run("SpecificStorage returns true if storage is known", func(t *testing.T) {
		got, err := c.SpecificStorage("default")
		assert.NoError(t, err)
		assert.Equal(t, true, got)

		got, err = c.SpecificStorage("failover_2")
		assert.NoError(t, err)
		assert.Equal(t, true, got)
	})

	t.Run("SpecificStorage returns false and error if storage is unknown", func(t *testing.T) {
		got, err := c.SpecificStorage("failover_4")
		assert.Error(t, err)
		assert.Equal(t, false, got)
	})
}
