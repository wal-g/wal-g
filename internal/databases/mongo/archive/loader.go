package archive

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var (
	_ = []Uploader{&StorageUploader{}, &DiscardUploader{}}
	_ = []Downloader{&StorageDownloader{}}
	_ = []Purger{&StoragePurger{}}
)

// Uploader defines interface to store mongodb backups and oplog archives
type Uploader interface {
	UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error // TODO: rename firstTS
	UploadGapArchive(err error, firstTS, lastTS models.Timestamp) error
	UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error
}

// Downloader defines interface to fetch mongodb oplog archives
type Downloader interface {
	BackupMeta(name string) (*models.Backup, error)
	DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error
	ListOplogArchives() ([]models.Archive, error)
	LoadBackups(names []string) ([]*models.Backup, error)
	ListBackups() ([]internal.BackupTime, []string, error)
	LastKnownArchiveTS() (models.Timestamp, error)
}

type Purger interface {
	DeleteBackups(backups []*models.Backup) error
	DeleteGarbage(garbage []string) error
	DeleteOplogArchives(archives []models.Archive) error
}

// StorageSettings defines storage relative paths
type StorageSettings struct {
	oplogsPath  string
	backupsPath string
}

// NewDefaultStorageSettings builds default storage settings struct
func NewDefaultStorageSettings() StorageSettings {
	return StorageSettings{
		oplogsPath:  models.OplogArchBasePath,
		backupsPath: utility.BaseBackupPath,
	}
}

// StorageDownloader extends base folder with mongodb specific.
type StorageDownloader struct {
	rootFolder    storage.Folder
	oplogsFolder  storage.Folder
	backupsFolder storage.Folder
}

// NewStorageDownloader builds mongodb downloader.
func NewStorageDownloader(opts StorageSettings) (*StorageDownloader, error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}
	return &StorageDownloader{rootFolder: folder,
			oplogsFolder:  folder.GetSubFolder(opts.oplogsPath),
			backupsFolder: folder.GetSubFolder(opts.backupsPath)},
		nil
}

// BackupMeta downloads sentinel contents.
func (sd *StorageDownloader) BackupMeta(name string) (*models.Backup, error) {
	return common.DownloadSentinel(sd.backupsFolder, name)
}

// LoadBackups downloads backups metadata
func (sd *StorageDownloader) LoadBackups(names []string) ([]*models.Backup, error) {
	backups := make([]*models.Backup, 0, len(names))
	for _, name := range names {
		backup, err := sd.BackupMeta(name)
		if err != nil {
			return nil, err
		}
		backups = append(backups, backup)
	}
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].FinishLocalTime.After(backups[j].FinishLocalTime)
	})
	return backups, nil
}

// ListBackups lists backups in folder
func (sd *StorageDownloader) ListBackups() ([]internal.BackupTime, []string, error) {
	return internal.GetBackupsAndGarbage(sd.backupsFolder)
}

// LastBackupName get last backup
func (sd *StorageDownloader) LastBackupName() (string, error) {
	backup, err := internal.GetLatestBackup(sd.backupsFolder)
	if err != nil {
		return "", err
	}
	return backup.Name, nil
}

// DownloadOplogArchive downloads, decompresses and decrypts (if needed) oplog archive.
func (sd *StorageDownloader) DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error {
	return internal.DownloadFile(internal.NewFolderReader(sd.oplogsFolder), arch.Filename(), arch.Extension(), writeCloser)
}

// ListOplogArchives fetches all oplog archives existed in storage.
func (sd *StorageDownloader) ListOplogArchives() ([]models.Archive, error) {
	objects, _, err := sd.oplogsFolder.ListFolder()
	if err != nil {
		return nil, fmt.Errorf("can not list oplog archives folder: %w", err)
	}

	archives := make([]models.Archive, 0, len(objects))
	for _, key := range objects {
		archName := key.GetName()
		arch, err := models.ArchFromFilename(archName)
		if err != nil {
			return nil, fmt.Errorf("can not convert retrieve timestamps since oplog archive Ext '%s': %w", archName, err)
		}
		archives = append(archives, arch)
	}
	return archives, nil
}

// LastKnownArchiveTS returns the most recent existed timestamp in storage folder.
func (sd *StorageDownloader) LastKnownArchiveTS() (models.Timestamp, error) {
	maxTS := models.Timestamp{}
	keys, _, err := sd.oplogsFolder.ListFolder()
	if err != nil {
		return models.Timestamp{}, fmt.Errorf("can not fetch keys since storage folder: %w ", err)
	}
	for _, key := range keys {
		filename := key.GetName()
		arch, err := models.ArchFromFilename(filename)
		if err != nil {
			return models.Timestamp{}, fmt.Errorf("can not build archive since filename '%s': %w", filename, err)
		}
		maxTS = models.MaxTS(maxTS, arch.End)
	}
	return maxTS, nil
}

// DiscardUploader reads provided data and returns success
type DiscardUploader struct {
	compressor compression.Compressor
	readerFrom io.ReaderFrom
}

// NewDiscardUploader builds DiscardUploader.
func NewDiscardUploader(compressor compression.Compressor, readerFrom io.ReaderFrom) *DiscardUploader {
	return &DiscardUploader{compressor, readerFrom}
}

// UploadOplogArchive reads all data into memory, stream is compressed and encrypted if required
func (d *DiscardUploader) UploadOplogArchive(archReader io.Reader, firstTS, lastTS models.Timestamp) error {
	if d.compressor != nil {
		archReader = internal.CompressAndEncrypt(archReader, d.compressor, internal.ConfigureCrypter())
	}
	if d.readerFrom != nil {
		if _, err := d.readerFrom.ReadFrom(archReader); err != nil {
			return err
		}
	}

	return nil
}

// UploadGapArchive returns nil error
func (d *DiscardUploader) UploadGapArchive(err error, firstTS, lastTS models.Timestamp) error {
	return nil
}

// UploadBackup is not implemented yet
func (d *DiscardUploader) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error {
	panic("implement me")
}

// StorageUploader extends base uploader with mongodb specific.
// is NOT thread-safe
type StorageUploader struct {
	internal.Uploader
	crypter crypto.Crypter // usages only in UploadOplogArchive
	buf     *bytes.Buffer
}

// NewStorageUploader builds mongodb uploader.
func NewStorageUploader(upl internal.Uploader) *StorageUploader {
	upl.DisableSizeTracking() // providing io.ReaderAt+io.ReadSeeker to s3 upload enables buffer pool usage
	return &StorageUploader{upl, internal.ConfigureCrypter(), &bytes.Buffer{}}
}

// UploadOplogArchive compresses a stream and uploads it with given archive name.
func (su *StorageUploader) UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error {
	arch, err := models.NewArchive(firstTS, lastTS, su.Compression().FileExtension(), models.ArchiveTypeOplog)
	if err != nil {
		return fmt.Errorf("can not build archive: %w", err)
	}

	_, err = su.buf.ReadFrom(internal.CompressAndEncrypt(stream, su.Uploader.Compression(), su.crypter))
	// TODO: warn if read > 2 * models.MaxDocumentSize and shrink buf capacity if it's too high
	defer su.buf.Reset()
	if err != nil {
		return err
	}

	// providing io.ReaderAt+io.ReadSeeker to s3 upload enables buffer pool usage
	return su.Upload(arch.Filename(), bytes.NewReader(su.buf.Bytes()))
}

// UploadGap uploads mark indicating archiving gap.
func (su *StorageUploader) UploadGapArchive(archErr error, firstTS, lastTS models.Timestamp) error {
	if archErr == nil {
		return fmt.Errorf("archErr must not be nil")
	}

	arch, err := models.NewArchive(firstTS, lastTS, su.Compression().FileExtension(), models.ArchiveTypeGap)
	if err != nil {
		return fmt.Errorf("can not build archive: %w", err)
	}

	if err := su.PushStreamToDestination(strings.NewReader(archErr.Error()), arch.Filename()); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	return nil
}

// UploadBackup compresses a stream and uploads it.
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error {
	err := metaConstructor.Init()
	if err != nil {
		return fmt.Errorf("can not init meta provider: %+v", err)
	}
	backupName, err := su.PushStream(stream)
	if err != nil {
		return fmt.Errorf("can not push stream: %+v", err)
	}

	if err := metaConstructor.Finalize(backupName); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("backup command failed: %+v", err)
	}

	backupSentinel := metaConstructor.MetaInfo()
	if err := internal.UploadSentinel(su.Uploader, backupSentinel, backupName); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}

// StoragePurger deletes files in storage.
type StoragePurger struct {
	oplogsFolder  storage.Folder
	backupsFolder storage.Folder
}

// NewStoragePurger builds mongodb StoragePurger.
func NewStoragePurger(opts StorageSettings) (*StoragePurger, error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}

	return &StoragePurger{oplogsFolder: folder.GetSubFolder(opts.oplogsPath),
		backupsFolder: folder.GetSubFolder(opts.backupsPath)}, nil
}

// DeleteBackups purges given backups files
// TODO: extract BackupLayout abstraction and provide DataPath(), SentinelPath(), Exists() methods
func (sp *StoragePurger) DeleteBackups(backups []*models.Backup) error {
	backupNames := BackupNamesFromBackups(backups)
	return internal.DeleteBackups(sp.backupsFolder, backupNames)
}

// DeleteGarbage purges given garbage keys
func (sp *StoragePurger) DeleteGarbage(garbage []string) error {
	return internal.DeleteGarbage(sp.backupsFolder, garbage)
}

// DeleteOplogArchives purges given oplogs files
func (sp *StoragePurger) DeleteOplogArchives(archives []models.Archive) error {
	oplogKeys := make([]string, 0, len(archives))
	for _, arch := range archives {
		oplogKeys = append(oplogKeys, arch.Filename())
	}
	tracelog.DebugLogger.Printf("Oplog keys will be deleted: %+v\n", oplogKeys)
	return sp.oplogsFolder.DeleteObjects(oplogKeys)
}
