package archive

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Backup represents backup sentinel data
type Backup struct {
	BackupName      string      `json:"BackupName,omitempty"`
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	Permanent       bool        `json:"Permanent"`
	DataSize        int64       `json:"DataSize,omitempty"`
	BackupSize      int64       `json:"BackupSize,omitempty"`
}

func (b Backup) Name() string {
	return b.BackupName
}

func (b Backup) StartTime() time.Time {
	return b.StartLocalTime
}

func (b Backup) IsPermanent() bool {
	return b.Permanent
}

func SplitRedisBackups(backups []Backup, purgeBackups, retainBackups map[string]bool) (purge, retain []Backup) {
	for _, backup := range backups {
		if purgeBackups[backup.Name()] {
			purge = append(purge, backup)
			continue
		}
		if retainBackups[backup.Name()] {
			retain = append(retain, backup)
		}
	}
	return purge, retain
}

func RedisModelToTimedBackup(backups []Backup) []internal.TimedBackup {
	if backups == nil {
		return nil
	}
	result := make([]internal.TimedBackup, len(backups))
	for i := range backups {
		result[i] = backups[i]
	}
	return result
}

// BackupMeta stores the data needed to create a Backup json object
type BackupMeta struct {
	DataSize       int64
	CompressedSize int64
	Permanent      bool
	User           interface{}
	StartTime      time.Time
	FinishTime     time.Time
}

type RedisMetaConstructor struct {
	ctx       context.Context
	folder    storage.Folder
	meta      BackupMeta
	permanent bool
}

// Init - required for internal.MetaConstructor
func (m *RedisMetaConstructor) Init() error {
	m.meta = BackupMeta{
		Permanent: m.permanent,
		User:      internal.GetSentinelUserData(),
		StartTime: utility.TimeNowCrossPlatformLocal(),
	}
	return nil
}

func (m *RedisMetaConstructor) MetaInfo() interface{} {
	meta := m.meta
	return &Backup{
		Permanent:       meta.Permanent,
		UserData:        meta.User,
		StartLocalTime:  meta.StartTime,
		FinishLocalTime: meta.FinishTime,
	}
}

func (m *RedisMetaConstructor) Finalize(backupName string) error {
	m.meta.FinishTime = utility.TimeNowCrossPlatformLocal()
	return nil
}

func NewBackupRedisMetaConstructor(ctx context.Context, folder storage.Folder, permanent bool) internal.MetaConstructor {
	return &RedisMetaConstructor{ctx: ctx, folder: folder, permanent: permanent}
}

type StorageUploader struct {
	internal.UploaderProvider
}

// NewRedisStorageUploader builds redis uploader, that also push metadata
func NewRedisStorageUploader(upl *internal.Uploader) *StorageUploader {
	return &StorageUploader{upl}
}

// UploadBackup compresses a stream and uploads it, and uploads meta info
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaConstructor internal.MetaConstructor) error {
	err := metaConstructor.Init()
	if err != nil {
		return fmt.Errorf("can not init meta provider: %+v", err)
	}

	dstPath, err := su.PushStream(stream)
	if err != nil {
		return fmt.Errorf("can not upload backup: %+v", err)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("backup command failed: %+v", err)
	}

	if err := metaConstructor.Finalize(dstPath); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	backupSentinelInfo := metaConstructor.MetaInfo()

	uploadedSize, uploadedErr := su.UploadedDataSize()
	rawSize, rawErr := su.RawDataSize()
	if uploadedErr != nil || rawErr != nil {
		return fmt.Errorf("can not calc backup size: %+v", rawErr)
	}

	backup := backupSentinelInfo.(*Backup)
	backup.BackupSize = uploadedSize
	backup.BackupName = dstPath
	backup.DataSize = rawSize
	if err := internal.UploadSentinel(su, backupSentinelInfo, dstPath); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}
