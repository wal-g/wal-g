package internal_test

import (
	"bytes"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func init() {
	internal.ConfigureSettings("")
	conf.InitConfig()
	conf.Configure()
}

var testBackup = internal.GenericMetadata{
	BackupName:       "metadata",
	UncompressedSize: int64(10),
	CompressedSize:   int64(100),
	Hostname:         "TestHost",
	StartTime:        time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
	FinishTime:       time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC),
	IsPermanent:      false,
	UserData:         "Data",
}

func convertMetadataFetch(input internal.GenericMetadata) map[string]interface{} {
	metadata := map[string]interface{}{
		"BackupName":       input.BackupName,
		"UncompressedSize": input.UncompressedSize,
		"CompressedSize":   input.CompressedSize,
		"Hostname":         input.Hostname,
		"StartTime":        input.StartTime,
		"FinishTime":       input.FinishTime,
		"IsPermanent":      input.IsPermanent,
		"UserData":         input.UserData,
		"start_time":       input.StartTime,
		"backup_name":      input.BackupName,
		"is_permanent":     input.IsPermanent,
		"user_data":        input.UserData,
	}
	return metadata
}

func copyMetadata(input internal.GenericMetadata) internal.GenericMetadata {
	metadata := internal.GenericMetadata{
		BackupName:       input.BackupName,
		UncompressedSize: input.UncompressedSize,
		CompressedSize:   input.CompressedSize,
		Hostname:         input.Hostname,
		StartTime:        input.StartTime,
		FinishTime:       input.FinishTime,
		IsPermanent:      input.IsPermanent,
		UserData:         input.UserData,
	}
	return metadata
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

	b := path.Join(utility.BaseBackupPath, testBackup.BackupName+".json")
	meta := convertMetadataFetch(testBackup)
	bytesMeta, _ := json.Marshal(&meta)
	_ = folder.PutObject(b, strings.NewReader(string(bytesMeta)))

	backup, err0 := internal.GetBackupByName("base_123", utility.BaseBackupPath, folder)
	assert.NoError(t, err0)

	err := backup.FetchMetadata(&meta)

	// Проверка результата
	assert.NoError(t, err)
	bytesMeta2, _ := json.Marshal(&meta)
	t.Logf(string(bytesMeta2))
	assert.Equal(t, testBackup.BackupName, meta["BackupName"])

	//assert.Equal(t, testBackup.UncompressedSize, meta.UncompressedSize)

	//assert.Equal(t, testBackup.CompressedSize, meta.CompressedSize)

	//assert.Equal(t, testBackup, meta)
}
