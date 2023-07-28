package multistorage

import (
	"sort"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const DefaultStorage = "default"

type FailoverFolder struct {
	storage.Folder
	Name string
}

func NewDefaultFailoverFolder(folder storage.Folder) FailoverFolder {
	return FailoverFolder{
		Folder: folder,
		Name:   DefaultStorage,
	}
}

func NewFailoverFolders(base storage.Folder, failovers map[string]storage.Folder) (storages []FailoverFolder) {
	storages = append(storages, FailoverFolder{
		Folder: base,
		Name:   DefaultStorage,
	})

	for name, folder := range failovers {
		storages = append(storages, FailoverFolder{
			Folder: folder,
			Name:   name,
		})
	}

	sort.Slice(storages, func(i, j int) bool {
		return storages[i].Name == DefaultStorage || storages[i].Name < storages[j].Name
	})

	return storages
}
