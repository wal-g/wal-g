package binary

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const FilesMetadataSuffix = "_backup_files.json"

type BackupStorage struct {
	UploaderProvider internal.UploaderProvider

	BackupName  string
	ReplSetName string
}

func CreateBackupStorage(backupName, replSetName string) (*BackupStorage, error) {
	uploaderProvider, err := internal.ConfigureSplitUploader()
	if err != nil {
		return nil, err
	}
	uploaderProvider.DisableSizeTracking()

	uploaderProvider.ChangeDirectory(utility.BaseBackupPath + "/")

	return &BackupStorage{
		UploaderProvider: uploaderProvider,
		BackupName:       backupName,
		ReplSetName:      replSetName,
	}, nil
}

func (backupStorage *BackupStorage) UploadMongodBackupSentinel(sentinel *MongodBackupSentinel) error {
	return internal.UploadSentinel(backupStorage.UploaderProvider, sentinel, backupStorage.BackupName)
}

func (backupStorage *BackupStorage) FilesMetadataNameFromBackup() string {
	return backupStorage.GetBackupDataAbsolutePath(backupStorage.BackupName + FilesMetadataSuffix)
}

func (backupStorage *BackupStorage) UploadMongodBackupFilesMetadata(filesMetadata *MongodBackupFilesMetadata) error {
	filesMetadataPath := backupStorage.FilesMetadataNameFromBackup()
	return internal.UploadDto(backupStorage.UploaderProvider.Folder(), filesMetadata, filesMetadataPath)
}

func (backupStorage *BackupStorage) DownloadMongodBackupSentinel() (*MongodBackupSentinel, error) {
	return DownloadSentinel(backupStorage.UploaderProvider.Folder(), backupStorage.BackupName)
}

func (backupStorage *BackupStorage) DownloadMongodBackupFilesMetadata() (*MongodBackupFilesMetadata, error) {
	var filesMetadata MongodBackupFilesMetadata
	filesMetadataPath := backupStorage.FilesMetadataNameFromBackup()
	err := internal.FetchDto(backupStorage.UploaderProvider.Folder(), filesMetadata, filesMetadataPath)
	if err != nil {
		return nil, errors.Wrap(err, "can not fetch files metadata")
	}
	return &filesMetadata, nil
}

func DownloadSentinel(folder storage.Folder, backupName string) (*MongodBackupSentinel, error) {
	var sentinel MongodBackupSentinel
	err := common.DownloadSentinel(folder, backupName, &sentinel)
	if err != nil {
		return nil, err
	}
	return &sentinel, nil
}

func (backupStorage *BackupStorage) GetBackupDataAbsolutePath(fileName string) string {
	return filepath.Join(backupStorage.BackupName, backupStorage.ReplSetName, fileName)
}

func (backupStorage *BackupStorage) MakeBackupFilePath(backupFileMeta *BackupFileMeta) string {
	return backupStorage.GetBackupDataAbsolutePath(backupFileMeta.Path + "." + backupFileMeta.Compression)
}

func (backupStorage *BackupStorage) CreateReader(backupFileMeta *BackupFileMeta) (io.ReadCloser, error) {
	objectPath := backupStorage.MakeBackupFilePath(backupFileMeta)
	objectReader, err := backupStorage.UploaderProvider.Folder().ReadObject(objectPath)
	if err != nil {
		return objectReader, err
	}
	decompressor := compression.FindDecompressor(backupFileMeta.Compression)
	if decompressor == nil {
		return nil, fmt.Errorf("decompressor for %v not found", backupFileMeta.Compression)
	}
	return internal.DecompressDecryptBytes(objectReader, decompressor)
}

func (backupStorage *BackupStorage) GetCompression() string {
	return backupStorage.UploaderProvider.Compression().FileExtension()
}

// todo: need multitraded upload
func (backupStorage *BackupStorage) UploadFile(reader io.Reader, backupFileMeta *BackupFileMeta) error {
	destinationFilePath := backupStorage.MakeBackupFilePath(backupFileMeta)

	return backupStorage.UploaderProvider.PushStreamToDestination(reader, destinationFilePath)
}

func (backupStorage *BackupStorage) CalculateCompressedFiles(backupFiles map[string]*BackupFileMeta) (int64, error) {
	folder := backupStorage.UploaderProvider.Folder().
		GetSubFolder(backupStorage.BackupName).
		GetSubFolder(backupStorage.ReplSetName)
	return backupStorage.calculateCompressedFiles("", folder, backupFiles)
}

//nolint: whitespace
func (backupStorage *BackupStorage) calculateCompressedFiles(relativePath string, folder storage.Folder,
	backupFiles map[string]*BackupFileMeta) (backupSize int64, err error) {

	objects, subFolders, err := folder.ListFolder()
	if err != nil {
		return 0, err
	}

	for _, object := range objects {
		objectPath := makeObjectPath(object.GetName(), relativePath)
		backupFilePath, compressExtension := splitOnFilePathAndCompressExtension(objectPath)
		backupFileMeta, ok := backupFiles[backupFilePath]
		if !ok {
			return 0, fmt.Errorf("unknown object: %v. known: %v", backupFilePath, backupFiles)
		}
		if backupFileMeta.Compression != compressExtension {
			return 0, fmt.Errorf("inconsistency! compression '%v' in path %v, but expected %v",
				compressExtension, filepath.Join(relativePath, object.GetName()), backupFileMeta.Compression)
		}
		backupFileMeta.CompressedSize = object.GetSize()
		backupSize += object.GetSize()
	}

	for _, subFolder := range subFolders {
		folderName := filepath.Base(subFolder.GetPath())
		relativeSubFolderPath := makeObjectPath(folderName, relativePath)
		subFolderSize, err := backupStorage.calculateCompressedFiles(relativeSubFolderPath, subFolder, backupFiles)
		if err != nil {
			return 0, err
		}
		backupSize += subFolderSize
	}

	return backupSize, nil
}

func splitOnFilePathAndCompressExtension(objectPath string) (filePath, extension string) {
	dotIndex := strings.LastIndex(objectPath, ".")
	if dotIndex < 0 {
		return objectPath, ""
	}
	compressionCodec := objectPath[dotIndex+1:]
	decompressor := compression.FindDecompressor(compressionCodec)
	if decompressor == nil {
		tracelog.WarningLogger.Printf("file path <%v> has unknown compression codec %v or don't compressed",
			objectPath, compressionCodec)
		return objectPath, ""
	}

	return objectPath[0:dotIndex], compressionCodec
}

func makeObjectPath(name, path string) string {
	if path == "" {
		return name
	}
	return filepath.Join(path, name)
}
