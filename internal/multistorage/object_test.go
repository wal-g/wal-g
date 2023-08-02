package multistorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestGetStorage(t *testing.T) {
	t.Run("provides storage name", func(t *testing.T) {
		obj := multiObject{
			Object:      storage.LocalObject{},
			storageName: "some_name",
		}
		name := GetStorage(obj)
		assert.Equal(t, "some_name", name)
	})

	t.Run("provides default name if object is not multiobject", func(t *testing.T) {
		obj := storage.LocalObject{}
		name := GetStorage(obj)
		assert.Equal(t, DefaultStorage, name)
	})
}
