package multistorage

import (
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Object = multiObject{}

type multiObject struct {
	storage.Object
	storageName string
}

func GetStorage(obj storage.Object) string {
	if mo, ok := obj.(multiObject); ok {
		return mo.storageName
	}
	return DefaultStorage
}
