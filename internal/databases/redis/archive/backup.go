package archive

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
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

// BackupMeta stores the data needed to create a Backup json object
type BackupMeta struct {
	DataSize       int64
	CompressedSize int64
	Permanent      bool
	User           interface{}
	StartTime      time.Time
	FinishTime     time.Time
}

type RedisMetaDBProvider struct {
	ctx       context.Context
	folder    storage.Folder
	meta      BackupMeta
	permanent bool
}

// Init - required for internal.MetaProvider
func (m *RedisMetaDBProvider) Init() error {
	m.meta = BackupMeta{
		Permanent: m.permanent,
		User:      internal.GetSentinelUserData(),
		StartTime: utility.TimeNowCrossPlatformLocal(),
	}
	return nil
}

func (m *RedisMetaDBProvider) MetaInfo() interface{} {
	meta := m.meta
	return &Backup{
		Permanent:       meta.Permanent,
		UserData:        meta.User,
		StartLocalTime:  meta.StartTime,
		FinishLocalTime: meta.FinishTime,
	}
}

func (m *RedisMetaDBProvider) Finalize(backupName string) error {
	m.meta.FinishTime = utility.TimeNowCrossPlatformLocal()
	return nil
}

func NewBackupMetaRedisProvider(ctx context.Context, folder storage.Folder, permanent bool) internal.MetaProvider {
	return &RedisMetaDBProvider{ctx: ctx, folder: folder, permanent: permanent}
}

type StorageUploader struct {
	internal.UploaderProvider
}

// NewRedisStorageUploader builds redis uploader, that also push metadata
func NewRedisStorageUploader(upl *internal.Uploader) *StorageUploader {
	return &StorageUploader{upl}
}

// UploadBackup compresses a stream and uploads it, and uploads meta info
func (su *StorageUploader) UploadBackup(stream io.Reader, cmd internal.ErrWaiter, metaProvider internal.MetaProvider) error {
	err := metaProvider.Init()
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

	if err := metaProvider.Finalize(dstPath); err != nil {
		return fmt.Errorf("can not finalize meta provider: %+v", err)
	}

	backupSentinelInfo := metaProvider.MetaInfo()

	backup := backupSentinelInfo.(*Backup)
	backup.BackupSize = *su.GetBackupSize()
	backup.BackupName = dstPath
	backup.DataSize = *su.GetDataSize()
	if err := internal.UploadSentinel(su, backupSentinelInfo, dstPath); err != nil {
		return fmt.Errorf("can not upload sentinel: %+v", err)
	}
	return nil
}
