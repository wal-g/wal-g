package storage

import (
	"time"
)

//go:generate mockgen -destination=../../../test/mocks/mock_object.go -package mocks -build_flags -mod=readonly github.com/wal-g/wal-g/pkg/storages/storage Object

type Object interface {
	GetName() string
	GetLastModified() time.Time
	GetSize() int64
}
