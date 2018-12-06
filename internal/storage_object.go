package internal

import "time"

type StorageObject interface {
	GetName() string
	GetLastModified() time.Time
}
