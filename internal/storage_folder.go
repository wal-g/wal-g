package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
)

type ObjectNotFoundError struct {
	error
}

func NewObjectNotFoundError(path string) ObjectNotFoundError {
	return ObjectNotFoundError{errors.Errorf("object '%s' not found in storage", path)}
}

func (err ObjectNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type StorageFolder interface {
	// Path should always ends with '/'
	GetPath() string

	// Should return objects with relative paths
	ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error)

	// Delete object, if exists
	DeleteObjects(objectRelativePaths []string) error

	Exists(objectRelativePath string) (bool, error)

	// Returns handle to subfolder. Does not have to instantiate subfolder in any material form
	GetSubFolder(subFolderRelativePath string) StorageFolder

	// Should return ObjectNotFoundError in case, there is no such object
	ReadObject(objectRelativePath string) (io.ReadCloser, error)

	PutObject(name string, content io.Reader) error
}
