package mongo

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
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
	backup, err := internal.NewBackup(backupFolder, backupName)
	if err != nil {
		return internal.GenericMetadata{}, err
	}
	var sentinel models.Backup
	if err = backup.FetchSentinel(&sentinel); err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:       backupName,
		UncompressedSize: sentinel.UncompressedSize,
		CompressedSize:   sentinel.CompressedSize,
		Hostname:         sentinel.Hostname,
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

func (ms GenericMetaSetter) SetUserData(backupName string, backupFolder storage.Folder, userData any) error {
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
	backup, err := internal.NewBackup(backupFolder, backupName)
	if err != nil {
		return err
	}

	var sentinel archive.Backup
	if err = backup.FetchSentinel(&sentinel); err != nil {
		return errors.Wrap(err, "failed to fetch the existing backup metadata for modifying")
	}
	sentinel = modifier(sentinel)
	if err = backup.UploadSentinel(sentinel); err != nil {
		return errors.Wrap(err, "failed to upload the modified metadata to the storage")
	}
	return nil
}
