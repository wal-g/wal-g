package internal

import (
	"time"

	"github.com/wal-g/storages/storage"
)

type GenericMetadata struct {
	BackupName       string
	UncompressedSize int64
	CompressedSize   int64
	Hostname         string
	StartTime        time.Time
	FinishTime       time.Time

	IsPermanent   bool
	IsIncremental bool

	// need to use separate fetcher
	// to avoid useless sentinel load (in Postgres)
	IncrementDetails IncrementDetailsFetcher

	UserData interface{}

	//todo: consider adding
	//SystemIdentifier *uint64
}

type IncrementDetails struct {
	IncrementFrom     string
	IncrementFullName string
	IncrementCount    int
}

type IncrementDetailsFetcher interface {
	Fetch() (bool, IncrementDetails, error)
}

type GenericMetaInteractor interface {
	GenericMetaFetcher
	GenericMetaSetter
}

type GenericMetaFetcher interface {
	Fetch(backupName string, backupFolder storage.Folder) (GenericMetadata, error)
}

type GenericMetaSetter interface {
	SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error
	SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error
}

// NopIncrementDetailsFetcher is useful for databases without incremental backup support
type NopIncrementDetailsFetcher struct{}

func (idf *NopIncrementDetailsFetcher) Fetch() (bool, IncrementDetails, error) {
	return false, IncrementDetails{}, nil
}
