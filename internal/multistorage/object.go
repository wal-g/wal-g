package multistorage

import (
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// StorageTeller is any object that can tell the name of the storage in which it is stored.
type StorageTeller interface {
	GetStorage() string
}

var _ StorageTeller = multiObject{}

// multiObject is an internal implementation of MultiObject that is provided from multistorage.Folder methods
// instead of the simple storage.Object.
type multiObject struct {
	storage.Object
	storageName string
}

func (mo multiObject) GetStorage() string {
	return mo.storageName
}

// GetStorage provides the name of the storage where the object is stored. If the object can't tell the storage name on
// its own, provides "default".
func GetStorage(obj storage.Object) string {
	if st, ok := obj.(StorageTeller); ok {
		return st.GetStorage()
	}
	return consts.DefaultStorage
}
