package greenplum

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
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

// TODO: Unit tests
func (mf GenericMetaFetcher) fetch(
	backupName string, backupFolder storage.Folder, specificStorage bool, storage string,
) (internal.GenericMetadata, error) {
	var backup internal.Backup
	var err error
	if specificStorage {
		backup, err = internal.NewBackupInStorage(backupFolder, backupName, storage)
	} else {
		backup, err = internal.NewBackup(backupFolder, backupName)
	}
	if err != nil {
		return internal.GenericMetadata{}, err
	}
	var sentinel BackupSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:       backupName,
		UncompressedSize: sentinel.UncompressedSize,
		CompressedSize:   sentinel.CompressedSize,
		Hostname:         sentinel.Hostname,
		StartTime:        sentinel.StartTime,
		FinishTime:       sentinel.FinishTime,
		IsPermanent:      sentinel.IsPermanent,
		IncrementDetails: &internal.NopIncrementDetailsFetcher{},
		UserData:         sentinel.UserData,
	}, nil
}

func (mf GenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	return mf.fetch(backupName, backupFolder, false, "")
}

func (mf GenericMetaFetcher) FetchFromStorage(
	backupName string, backupFolder storage.Folder, storage string) (internal.GenericMetadata, error) {
	return mf.fetch(backupName, backupFolder, true, storage)
}

type GenericMetaSetter struct{}

func NewGenericMetaSetter() GenericMetaSetter {
	return GenericMetaSetter{}
}

// TODO: Unit tests
func (ms GenericMetaSetter) SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error {
	modifier := func(dto BackupSentinelDto) BackupSentinelDto {
		dto.UserData = userData
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

// TODO: Unit tests
func (ms GenericMetaSetter) SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error {
	modifier := func(dto BackupSentinelDto) BackupSentinelDto {
		dto.IsPermanent = isPermanent
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

func modifyBackupSentinel(backupName string, backupFolder storage.Folder, modifier func(BackupSentinelDto) BackupSentinelDto) error {
	backup, err := internal.NewBackup(backupFolder, backupName)
	if err != nil {
		return errors.Wrap(err, "failed to modify metadata")
	}
	var sentinel BackupSentinelDto
	err = backup.FetchSentinel(&sentinel)
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
