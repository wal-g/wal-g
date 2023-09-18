package multistorage

import (
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// MultiObject is any storage.Object that can tell the name of the storage in which it is stored.
type MultiObject interface {
	storage.Object
	GetStorage() string
}

var _ MultiObject = multiObject{}

// multiObject is an internal implementation of MultiObject that is provided from multistorage.Folder methods
// instead of the simple storage.Object.
type multiObject struct {
	storage.Object
	StorageName string
}

func (mo multiObject) GetStorage() string {
	return mo.StorageName
}

// GetStorage provides the name of the storage where the object is stored. If the object can't tell the storage name on
// its own, provides "default".
func GetStorage(obj storage.Object) string {
	// TODO: Don't rely on different storage.Object implementations here (issue #TODO). Possible solutions:
	// 1. Use Golang recursion (may work slowly).
	// 2. Get rid of storage.RelativePathObject and maybe some other implementations: explicitly distinct objects that
	//    provide file name only in their GetName() and objects that provide relative paths (a lot of coding).
	// 3. Make up something else.
	if rpo, ok := obj.(storage.RelativePathObject); ok {
		obj = rpo.Object
	}
	if mo, ok := obj.(MultiObject); ok {
		return mo.GetStorage()
	}
	return consts.DefaultStorage
}
