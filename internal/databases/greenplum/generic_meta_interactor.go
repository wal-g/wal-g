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

//TODO: Unit tests
func (mf GenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	var backup = internal.NewBackup(backupFolder, backupName)
	var sentinel BackupSentinelDto
	err := backup.FetchSentinel(&sentinel)
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

type GenericMetaSetter struct{}

func NewGenericMetaSetter() GenericMetaSetter {
	return GenericMetaSetter{}
}

//TODO: Unit tests
func (ms GenericMetaSetter) SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error {
	modifier := func(dto BackupSentinelDto) BackupSentinelDto {
		dto.UserData = userData
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

//TODO: Unit tests
func (ms GenericMetaSetter) SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error {
	modifier := func(dto BackupSentinelDto) BackupSentinelDto {
		dto.IsPermanent = isPermanent
		return dto
	}
	return modifyBackupSentinel(backupName, backupFolder, modifier)
}

func modifyBackupSentinel(backupName string, backupFolder storage.Folder, modifier func(BackupSentinelDto) BackupSentinelDto) error {
	backup := internal.NewBackup(backupFolder, backupName)
	var sentinel BackupSentinelDto
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
