package archive

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/storages/storage"
)

var (
	_ = []Uploader{&StorageUploader{}, &DiscardUploader{}}
	_ = []Downloader{&StorageDownloader{}}
	_ = []Purger{&StoragePurger{}}
)

// ErrWaiter
type ErrWaiter interface {
	Wait() error
}

// Uploader defines interface to store mongodb backups and oplog archives
type Uploader interface {
	UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error // TODO: rename firstTS
	UploadGapArchive(err error, firstTS, lastTS models.Timestamp) error
	UploadBackup(stream io.Reader, cmd ErrWaiter, metaProvider MongoMetaProvider) error
	FileExtension() string
}

// Downloader defines interface to fetch mongodb oplog archives
type Downloader interface {
	BackupMeta(name string) (Backup, error)
	DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error
	ListOplogArchives() ([]models.Archive, error)
	LoadBackups(names []string) ([]Backup, error)
	ListBackupNames() ([]internal.BackupTime, error)
}

type Purger interface {
	DeleteBackups(backups []Backup) error
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
	oplogsFolder  storage.Folder
	backupsFolder storage.Folder
}

// NewStorageDownloader builds mongodb downloader.
func NewStorageDownloader(opts StorageSettings) (*StorageDownloader, error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}
	return &StorageDownloader{oplogsFolder: folder.GetSubFolder(opts.oplogsPath), backupsFolder: folder.GetSubFolder(opts.backupsPath)}, nil
}

// BackupMeta downloads sentinel contents.
func (sd *StorageDownloader) BackupMeta(name string) (Backup, error) {
	backup := internal.NewBackup(sd.backupsFolder, name)
	var sentinel Backup
	err := internal.FetchStreamSentinel(backup, &sentinel)
	if err != nil {
		return Backup{}, fmt.Errorf("can not fetch stream sentinel: %w", err)
	}
	if sentinel.BackupName == "" {
		sentinel.BackupName = name
	}
	return sentinel, nil
}

// LoadBackups downloads backups metadata
func (sd *StorageDownloader) LoadBackups(names []string) ([]Backup, error) {
	backups := make([]Backup, 0, len(names))
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

// ListBackupNames lists backups in folder
func (sd *StorageDownloader) ListBackupNames() ([]internal.BackupTime, error) {
	backupObjects, _, err := sd.backupsFolder.ListFolder()
	if err != nil {
		return nil, err
	}
	sortTimes := make([]internal.BackupTime, 0, len(backupObjects))
	for _, object := range backupObjects {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		mtime := object.GetLastModified()
		sortTimes = append(sortTimes, internal.BackupTime{BackupName: utility.StripBackupName(key), Time: mtime, WalFileName: utility.StripWalFileName(key)})
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes, nil
}

// DownloadOplogArchive downloads, decompresses and decrypts (if needed) oplog archive.
func (sd *StorageDownloader) DownloadOplogArchive(arch models.Archive, writeCloser io.WriteCloser) error {
	return internal.DownloadFile(sd.oplogsFolder, arch.Filename(), arch.Extension(), writeCloser)
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
func (d *DiscardUploader) UploadBackup(stream io.Reader, cmd ErrWaiter, metaProvider MongoMetaProvider) error {
	panic("implement me")
}

// FileExtension returns configured extension
func (d *DiscardUploader) FileExtension() string {
	return d.compressor.FileExtension()
}

// StorageUploader extends base uploader with mongodb specific.
type StorageUploader struct {
	internal.UploaderProvider
}

// NewStorageUploader builds mongodb uploader.
func NewStorageUploader(upl internal.UploaderProvider) *StorageUploader {
	return &StorageUploader{upl}
}

// UploadOplogArchive compresses a stream and uploads it with given archive name.
func (su *StorageUploader) UploadOplogArchive(stream io.Reader, firstTS, lastTS models.Timestamp) error {
	arch, err := models.NewArchive(firstTS, lastTS, su.FileExtension(), models.ArchiveTypeOplog)
	if err != nil {
		return fmt.Errorf("can not build archive: %w", err)
	}

	if err := su.PushStreamToDestination(stream, arch.Filename()); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	return nil
}

// UploadGap uploads mark indicating archiving gap.
func (su *StorageUploader) UploadGapArchive(archErr error, firstTS, lastTS models.Timestamp) error {
	if archErr == nil {
		return fmt.Errorf("archErr must not be nil")
	}

	arch, err := models.NewArchive(firstTS, lastTS, su.FileExtension(), models.ArchiveTypeGap)
	if err != nil {
		return fmt.Errorf("can not build archive: %w", err)
	}

	if err := su.PushStreamToDestination(strings.NewReader(archErr.Error()), arch.Filename()); err != nil {
		return fmt.Errorf("error while uploading stream: %w", err)
	}
	return nil
}

// UploadBackup compresses a stream and uploads it.
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd ErrWaiter, metaProvider MongoMetaProvider) error {
	timeStart := utility.TimeNowCrossPlatformLocal()
	backupName, err := su.PushStream(stream)
	if err != nil {
		return err
	}

	if err := metaProvider.Finalize(); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	backupSentinel := &Backup{
		StartLocalTime:  timeStart,
		FinishLocalTime: utility.TimeNowCrossPlatformLocal(),
		UserData:        internal.GetSentinelUserData(),
		MongoMeta:       metaProvider.Meta(),
	}
	return internal.UploadSentinel(su.UploaderProvider, backupSentinel, backupName)
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

	return &StoragePurger{oplogsFolder: folder.GetSubFolder(opts.oplogsPath), backupsFolder: folder.GetSubFolder(opts.backupsPath)}, nil
}

// DeleteBackups purges given backups files
func (sp *StoragePurger) DeleteBackups(backups []Backup) error {
	keys := make([]string, 0, len(backups)*2)
	for _, backup := range backups {
		b := internal.NewBackup(sp.backupsFolder, backup.BackupName)
		dataKeys, err := b.GetTarNames()
		if err != nil {
			return err
		}
		keys = append(keys, dataKeys...)
		keys = append(keys, b.GetStopSentinelPath())
	}

	if err := sp.backupsFolder.DeleteObjects(keys); err != nil {
		return err
	}
	return nil
}

// DeleteOplogArchives purges given oplogs files
func (sp *StoragePurger) DeleteOplogArchives(archives []models.Archive) error {
	oplogKeys := make([]string, 0, len(archives))
	for _, arch := range archives {
		oplogKeys = append(oplogKeys, arch.Filename())
	}
	return sp.oplogsFolder.DeleteObjects(oplogKeys)
}
