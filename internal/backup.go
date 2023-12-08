package internal

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// region errors
type SentinelMarshallingError struct {
	error
}

func NewSentinelMarshallingError(sentinelName string, err error) SentinelMarshallingError {
	return SentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err SentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

//endregion

// Backup provides basic functionality to fetch backup-related information from storages.
//
// WAL-G stores information about single backup in the following files:
//
// Sentinel file - contains useful information, such as backup start time, backup size, etc.
// see FetchSentinel, UploadSentinel
//
// Metadata file (only in Postgres) - Postgres sentinel files can be quite large (> 1GB),
// so the metadata file is useful for the quick fetch of backup-related information.
// see FetchMetadata, UploadMetadata
//
// Metadata file (only for MySQL) - stores information about backup format.
// WAL-G uses it to understand how to handle backup files during restore operations.
//
// All files of the backup must be stored in a single storage.
type Backup struct {
	Name string
	// base backup folder or catchup backup folder
	Folder storage.Folder
}

func NewBackup(folder storage.Folder, name string) (Backup, error) {
	err := multistorage.EnsureSingleStorageIsUsed(folder)
	if err != nil {
		return Backup{}, fmt.Errorf("create backup %s: %w", name, err)
	}
	return Backup{
		Name:   name,
		Folder: folder,
	}, nil
}

func NewBackupInStorage(folder storage.Folder, name, storage string) (Backup, error) {
	folder, err := multistorage.UseSpecificStorage(storage, folder)
	if err != nil {
		return Backup{}, fmt.Errorf("create backup %s in storage %s: %w", name, storage, err)
	}
	return Backup{
		Name:   name,
		Folder: folder,
	}, nil
}

// getStopSentinelPath returns sentinel path.
func (backup *Backup) getStopSentinelPath() string {
	return SentinelNameFromBackup(backup.Name)
}

func (backup *Backup) getMetadataPath() string {
	return backup.Name + "/" + utility.MetadataFileName
}

// SentinelExists checks that the sentinel file of the specified backup exists.
func (backup *Backup) SentinelExists() (bool, error) {
	return backup.Folder.Exists(backup.getStopSentinelPath())
}

// TODO : unit tests
func (backup *Backup) FetchSentinel(sentinelDto interface{}) error {
	return FetchDto(backup.Folder, sentinelDto, backup.getStopSentinelPath())
}

// TODO : unit tests
func (backup *Backup) FetchMetadata(metadataDto interface{}) error {
	return FetchDto(backup.Folder, metadataDto, backup.getMetadataPath())
}

func (backup *Backup) UploadMetadata(metadataDto interface{}) error {
	return UploadDto(backup.Folder, metadataDto, backup.getMetadataPath())
}

func (backup *Backup) UploadSentinel(sentinelDto interface{}) error {
	return UploadDto(backup.Folder, sentinelDto, backup.getStopSentinelPath())
}

// FetchDto gets data from path and de-serializes it to given object
func FetchDto(folder storage.Folder, dto interface{}, path string) error {
	backupReaderMaker := NewStorageReaderMaker(folder, path)
	reader, err := backupReaderMaker.Reader()
	if err != nil {
		return err
	}
	unmarshaller, err := NewDtoSerializer()
	if err != nil {
		return err
	}
	return errors.Wrap(unmarshaller.Unmarshal(reader, dto), fmt.Sprintf("failed to fetch dto from %s", path))
}

// UploadDto serializes given object to JSON and puts it to path
func UploadDto(folder storage.Folder, dto interface{}, path string) error {
	marshaller, err := NewDtoSerializer()
	if err != nil {
		return err
	}
	r, err := marshaller.Marshal(dto)
	if err != nil {
		return err
	}
	return folder.PutObject(path, r)
}

func (backup *Backup) CheckExistence() (bool, error) {
	exists, err := backup.SentinelExists()
	if err != nil {
		return false, errors.Wrap(err, "failed to check if backup sentinel exists")
	}
	return exists, nil
}

// AssureExists is similar to CheckExistence, but returns
// an error in two cases:
// 1. Backup does not exist
// 2. Failed to check if backup exist
func (backup *Backup) AssureExists() error {
	exists, err := backup.CheckExistence()
	if err != nil {
		return err
	}
	if !exists {
		return NewBackupNonExistenceError(backup.Name)
	}
	return nil
}

func (backup *Backup) GetStorageName() string {
	return multistorage.UsedStorages(backup.Folder)[0]
}

// TODO : unit tests
func UploadSentinel(uploader Uploader, sentinelDto interface{}, backupName string) error {
	sentinelName := SentinelNameFromBackup(backupName)
	return UploadDto(uploader.Folder(), sentinelDto, sentinelName)
}

type ErrWaiter interface {
	Wait() error
}

// MetaConstructor - interface that helps with building meta-info about backup and generate MetaInfo
// see MongoMetaConstructor
// see RedisMetaConstructor
type MetaConstructor interface {
	Init() error
	Finalize(backupName string) error
	MetaInfo() interface{}
}
