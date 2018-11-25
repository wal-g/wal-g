package walg

import "time"

type StorageObject interface {
	GetAbsPath() string
	GetLastModified() time.Time
}
