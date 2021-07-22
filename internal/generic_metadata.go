package internal

import (
	"time"

	"github.com/wal-g/wal-g/internal/storages/storage"
)

// GenericMetadata allows to obtain some basic information
// about existing backup in storage. It is useful when
// creating a functionality that is common to all databases,
// for example backup-list or backup-mark.
//
// To support the GenericMetadata in some particular database,
// one should write its own GenericMetaFetcher and GenericMetaSetter.
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
}

// IncrementDetails is useful to fetch information about
// dependencies of some incremental backup
type IncrementDetails struct {
	IncrementFrom     string
	IncrementFullName string
	IncrementCount    int
}

type IncrementDetailsFetcher interface {
	Fetch() (isIncremental bool, details IncrementDetails, err error)
}

// GenericMetaInteractor is a combination of GenericMetaFetcher
// and GenericMetaSetter. It can be useful when need both.
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
