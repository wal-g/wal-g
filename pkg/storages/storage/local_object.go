package storage

import (
	"io/fs"
	"time"
)

var _ Object = LocalObject{}

type LocalObject struct {
	name           string
	lastModified   time.Time
	size           int64
	versionID      string
	additionalInfo string
}

func NewLocalObject(name string, lastModified time.Time, size int64) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size}
}

const SymlinkInfo = "SYMLINK"

func NewLocalObjectFromInfo(info fs.FileInfo) *LocalObject {
	object := NewLocalObject(info.Name(), info.ModTime(), info.Size())
	if info.Mode()&fs.ModeSymlink != 0 {
		object.additionalInfo = SymlinkInfo
	}
	return object
}

func NewLocalObjectWithVersion(name string, lastModified time.Time, size int64, version string, additionalInfo string) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size, versionID: version, additionalInfo: additionalInfo}
}

func (object LocalObject) GetName() string {
	return object.name
}

func (object LocalObject) GetLastModified() time.Time {
	return object.lastModified
}

func (object LocalObject) GetSize() int64 {
	return object.size
}

func (object LocalObject) GetVersionID() string {
	return object.versionID
}

func (object LocalObject) GetAdditionalInfo() string {
	return object.additionalInfo
}
