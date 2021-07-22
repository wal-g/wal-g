package storage

import "time"

type LocalObject struct {
	name         string
	lastModified time.Time
	size 		 int64
}

func NewLocalObject(name string, lastModified time.Time, size int64) *LocalObject {
	return &LocalObject{name, lastModified, size}
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
