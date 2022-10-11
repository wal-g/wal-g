package binary

import (
	"fmt"
	"io"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
)

const FilesMetadataFileName = "_backup_files.json"

type BackupStorage struct {
	Uploader   *internal.Uploader
	BackupName string
}

func CreateBackupStorage(backupName string) (*BackupStorage, error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return nil, err
	}

	uploader.ChangeDirectory(utility.BaseBackupPath + "/")

	return &BackupStorage{
		Uploader:   uploader,
		BackupName: backupName,
	}, nil
}

func (backupStorage *BackupStorage) UploadSentinel(sentinel *models.Backup) error {
	return internal.UploadSentinel(backupStorage.Uploader, sentinel, backupStorage.BackupName)
}

func (backupStorage *BackupStorage) UploadMongodBackupFilesMetadata(filesMetadata *MongodBackupFilesMetadata) error {
	filesMetadataPath := backupStorage.FilesMetadataNameFromBackup()
	return internal.UploadDto(backupStorage.Uploader.Folder(), filesMetadata, filesMetadataPath)
}

func (backupStorage *BackupStorage) DownloadSentinel() (*models.Backup, error) {
	return common.DownloadSentinel(backupStorage.Uploader.Folder(), backupStorage.BackupName)
}

func (backupStorage *BackupStorage) DownloadMongodBackupFilesMetadata() (*MongodBackupFilesMetadata, error) {
	var filesMetadata MongodBackupFilesMetadata
	filesMetadataPath := backupStorage.FilesMetadataNameFromBackup()
	err := internal.FetchDto(backupStorage.Uploader.Folder(), &filesMetadata, filesMetadataPath)
	if err != nil {
		return nil, errors.Wrap(err, "can not fetch files metadata")
	}
	return &filesMetadata, nil
}

func (backupStorage *BackupStorage) FilesMetadataNameFromBackup() string {
	return filepath.Join(backupStorage.BackupName, FilesMetadataFileName)
}

func (backupStorage *BackupStorage) MakeBackupFilePath(backupFileMeta *BackupFileMeta) string {
	return filepath.Join(backupStorage.BackupName, backupFileMeta.Path+"."+backupFileMeta.Compression)
}

func (backupStorage *BackupStorage) CreateReader(backupFileMeta *BackupFileMeta) (io.ReadCloser, error) {
	objectPath := backupStorage.MakeBackupFilePath(backupFileMeta)
	objectReader, err := backupStorage.Uploader.Folder().ReadObject(objectPath)
	if err != nil {
		return objectReader, err
	}
	reader := limiters.NewNetworkLimitReader(objectReader)
	decompressor := compression.FindDecompressor(backupFileMeta.Compression)
	if decompressor == nil {
		return nil, fmt.Errorf("decompressor for %v not found", backupFileMeta.Compression)
	}
	return internal.DecompressDecryptBytes(reader, decompressor)
}

func (backupStorage *BackupStorage) GetCompression() string {
	return backupStorage.Uploader.Compression().FileExtension()
}
