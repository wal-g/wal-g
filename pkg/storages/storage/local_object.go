package storage

import (
	"time"
)

var _ Object = LocalObject{}

type LocalObject struct {
	name           string
	lastModified   time.Time
	size           int64
	additionalInfo string
}

func NewLocalObject(name string, lastModified time.Time, size int64) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size}
}

func NewLocalObjectWithAdditionalInfo(name string, lastModified time.Time, size int64, info string) *LocalObject {
	return &LocalObject{name: name, lastModified: lastModified, size: size, additionalInfo: info}
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

func (object LocalObject) GetAdditionalInfo() string {
	return object.additionalInfo
}
