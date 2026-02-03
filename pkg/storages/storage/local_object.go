package storage

import (
	"time"
)

var _ Object = LocalObject{}

type LocalObject struct {
	name            string
	lastModified    time.Time
	size            int64
	versionId       string
	isVersionLatest string
}

func NewLocalObject(name string, lastModified time.Time, size int64) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size}
}

func NewLocalObjectWithVersion(name string, lastModified time.Time, size int64, version string, latest string) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size, versionId: version, isVersionLatest: latest}
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

func (object LocalObject) GetVersionId() string {
	return object.versionId
}

func (object LocalObject) GetIsVersionLatest() string {
	return object.isVersionLatest
}
