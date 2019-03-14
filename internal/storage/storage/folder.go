package storage

import (
	"io"
)

type Folder interface {
	// Path should always ends with '/'
	GetPath() string

	// Should return objects with relative paths
	ListFolder() (objects []Object, subFolders []Folder, err error)

	// Delete object, if exists
	DeleteObjects(objectRelativePaths []string) error

	Exists(objectRelativePath string) (bool, error)

	// Returns handle to subfolder. Does not have to instantiate subfolder in any material form
	GetSubFolder(subFolderRelativePath string) Folder

	// Should return ObjectNotFoundError in case, there is no such object
	ReadObject(objectRelativePath string) (io.ReadCloser, error)

	PutObject(name string, content io.Reader) error
}
