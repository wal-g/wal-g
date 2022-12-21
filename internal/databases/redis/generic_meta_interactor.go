package redis

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type GenericMetaInteractor struct {
	GenericMetaFetcher
	GenericMetaSetter
}

func NewGenericMetaInteractor() GenericMetaInteractor {
	return GenericMetaInteractor{
		GenericMetaFetcher: NewGenericMetaFetcher(),
		GenericMetaSetter:  NewGenericMetaSetter(),
	}
}

type GenericMetaFetcher struct{}

func NewGenericMetaFetcher() GenericMetaFetcher {
	return GenericMetaFetcher{}
}

func (mf GenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	var backup = internal.NewBackup(backupFolder, backupName)
	var sentinel archive.Backup
	err := backup.FetchSentinel(&sentinel)
	if err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:       backupName,
		UncompressedSize: sentinel.DataSize,
		CompressedSize:   sentinel.BackupSize,
		StartTime:        sentinel.StartLocalTime,
		FinishTime:       sentinel.FinishLocalTime,
		IsPermanent:      sentinel.Permanent,
		IncrementDetails: &internal.NopIncrementDetailsFetcher{},
		UserData:         sentinel.UserData,
	}, nil
}

type GenericMetaSetter struct{}

func NewGenericMetaSetter() GenericMetaSetter {
	return GenericMetaSetter{}
}

func (ms GenericMetaSetter) SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error {
	modifier := func(dto archive.Backup) archive.Backup {
		dto.UserData = userData
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

func (ms GenericMetaSetter) SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error {
	modifier := func(dto archive.Backup) archive.Backup {
		dto.Permanent = isPermanent
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

func modifyBackupSentinel(backupName string, backupFolder storage.Folder, modifier func(archive.Backup) archive.Backup) error {
	backup := internal.NewBackup(backupFolder, backupName)
	var sentinel archive.Backup
	err := backup.FetchSentinel(&sentinel)
	if err != nil {
		return errors.Wrap(err, "failed to fetch the existing backup metadata for modifying")
	}
	sentinel = modifier(sentinel)
	err = backup.UploadSentinel(sentinel)
	if err != nil {
		return errors.Wrap(err, "failed to upload the modified metadata to the storage")
	}
	return nil
}
