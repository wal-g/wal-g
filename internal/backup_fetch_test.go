package internal_test

import (
	"bytes"
	"path"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func init() {
	internal.ConfigureSettings("")
	conf.InitConfig()
	conf.Configure()
}

var testBackup = internal.GenericMetadata{
	BackupName:       "stream_20231118T140000Z",
	UncompressedSize: int64(10),
	CompressedSize:   int64(100),
	Hostname:         "TestHost",
	StartTime:        time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
	FinishTime:       time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
	IsPermanent:      false,
	UserData:         "Data",
}

func TestGetBackupByName_Latest(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
	assert.NoError(t, err)
	assert.Equal(t, folder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_000", backup.Name)
}

func TestGetBackupByName_LatestNoBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	folder.PutObject("folder123/nop", &bytes.Buffer{})
	_, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewNoBackupsFoundError(), err)
}

func TestGetBackupByName_Exists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup, err := internal.GetBackupByName("base_123", utility.BaseBackupPath, folder)
	assert.NoError(t, err)
	assert.Equal(t, folder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_123", backup.Name)
}

func TestGetBackupByName_NotExists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	_, err := internal.GetBackupByName("base_321", utility.BaseBackupPath, folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewBackupNonExistenceError(""), err)
}

func TestFetchMetadata(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()

	b := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+utility.SentinelSuffix)
	_ = folder.PutObject(b, &bytes.Buffer{})

	// Создание объекта Backup с помощью вспомогательной функции
	backup, err0 := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
	t.Logf("" + backup.Folder.GetPath());
	t.Logf("" + backup.GetMetadataPath());
	assert.NoError(t, err0)

	// Вызов функции FetchMetadata
	meta := internal.GenericMetadata{
		UncompressedSize: int64(10),
		CompressedSize:   int64(100),
		Hostname:         "TestHost",
		StartTime:        time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
		FinishTime:       time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
		IsPermanent:      false,
		UserData:         "Data",
	}

	err := backup.FetchMetadata(&meta)

	// Проверка результата
	assert.NoError(t, err)
	assert.Equal(t, testLatestBackup.BackupName, meta.BackupName)

	assert.Equal(t, testLatestBackup.UncompressedSize, meta.UncompressedSize)

	assert.Equal(t, testLatestBackup.CompressedSize, meta.CompressedSize)

	assert.Equal(t, testLatestBackup, meta)
}
