package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var testNamesInOrder = []string{"default", "failover_1", "failover_2"}

func TestAliveMap_FirstAlive(t *testing.T) {
	t.Run("provide first if alive", func(t *testing.T) {
		am := AliveMap{
			"default":    true,
			"failover_1": true,
			"failover_2": true,
		}
		name := am.FirstAlive(testNamesInOrder)
		defaultName := "default"
		assert.Equal(t, &defaultName, name)
	})

	t.Run("provide second if first is dead", func(t *testing.T) {
		am := AliveMap{
			"default":    false,
			"failover_1": true,
			"failover_2": true,
		}
		name := am.FirstAlive(testNamesInOrder)
		fo1Name := "failover_1"
		assert.Equal(t, &fo1Name, name)
	})

	t.Run("provide nil if all are dead", func(t *testing.T) {
		am := AliveMap{
			"default":    false,
			"failover_1": false,
			"failover_2": false,
		}
		name := am.FirstAlive(testNamesInOrder)
		assert.Nil(t, name)
	})

	t.Run("provide nil if map is empty", func(t *testing.T) {
		am := AliveMap{}
		name := am.FirstAlive(testNamesInOrder)
		assert.Nil(t, name)
	})

	t.Run("provide nil if storage list is empty", func(t *testing.T) {
		am := AliveMap{
			"default":    false,
			"failover_1": false,
			"failover_2": false,
		}
		name := am.FirstAlive(nil)
		assert.Nil(t, name)
	})
}

func TestAliveMap_AliveNames(t *testing.T) {
	t.Run("prodive only alive names in order", func(t *testing.T) {
		am := AliveMap{
			"default":    true,
			"failover_1": false,
			"failover_2": true,
		}
		names := am.AliveNames(testNamesInOrder)
		assert.Equal(t, []string{"default", "failover_2"}, names)
	})

	t.Run("provide empty when no alive", func(t *testing.T) {
		am := AliveMap{
			"default":    false,
			"failover_1": false,
			"failover_2": false,
		}
		names := am.AliveNames(testNamesInOrder)
		assert.Len(t, names, 0)
	})

	t.Run("provide empty when map is empty", func(t *testing.T) {
		am := AliveMap{}
		names := am.AliveNames(testNamesInOrder)
		assert.Len(t, names, 0)
	})

	t.Run("provide empty when storage list is empty", func(t *testing.T) {
		am := AliveMap{
			"default":    false,
			"failover_1": false,
			"failover_2": false,
		}
		names := am.AliveNames(nil)
		assert.Len(t, names, 0)
	})
}

func TestAliveMap_Names(t *testing.T) {
	t.Run("prodive all names in arbitrary order", func(t *testing.T) {
		am := AliveMap{
			"default":    true,
			"failover_1": false,
			"failover_2": true,
			"failover_3": false,
			"failover_4": true,
		}
		names := am.Names()
		wantNames := []string{"default", "failover_1", "failover_2", "failover_3", "failover_4"}
		assert.ElementsMatch(t, wantNames, names)
	})

	t.Run("provide empty slice when map is empty", func(t *testing.T) {
		am := AliveMap{}
		names := am.Names()
		assert.Len(t, names, 0)
	})
}
