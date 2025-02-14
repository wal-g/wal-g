package etcd

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type GenericMetaFetcher struct{}

func NewGenericMetaFetcher() GenericMetaFetcher {
	return GenericMetaFetcher{}
}

// TODO: Unit tests
func (mf GenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	backup, err := internal.NewBackup(backupFolder, backupName)
	if err != nil {
		return internal.GenericMetadata{}, err
	}
	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:  backupName,
		StartTime:   sentinel.StartLocalTime,
		IsPermanent: sentinel.IsPermanent,
		UserData:    sentinel.UserData,
	}, nil
}

// TODO implement fetch from storage in etcd
func (mf GenericMetaFetcher) FetchFromStorage(
	backupName string, backupFolder storage.Folder, storage string,
) (internal.GenericMetadata, error) {
	return mf.Fetch(backupName, backupFolder)
}
