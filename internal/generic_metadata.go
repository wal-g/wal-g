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

	// need to use separate func
	// because to avoid useless sentinel load (in Postgres)
	FetchIncrementDetails func() (bool, IncrementDetails, error)

	UserData interface{}

	//todo: consider adding
	//SystemIdentifier *uint64
}

type IncrementDetails struct {
	IncrementFrom     string
	IncrementFullName string
	IncrementCount    int
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
