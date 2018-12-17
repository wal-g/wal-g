package internal

import (
	"os"
	"time"
)

type FileStorageObject struct {
	os.FileInfo
}

func (object FileStorageObject) GetName() string {
	return object.Name()
}

func (object FileStorageObject) GetLastModified() time.Time {
	return object.ModTime()
}