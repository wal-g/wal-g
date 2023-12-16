package multistorage

import (
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type NamedFolder struct {
	storage.Folder
	StorageName string
}

func (nf NamedFolder) GetSubFolder(path string) NamedFolder {
	if path == "" {
		return nf
	}
	cpy := nf
	cpy.Folder = cpy.Folder.GetSubFolder(path)
	return cpy
}
