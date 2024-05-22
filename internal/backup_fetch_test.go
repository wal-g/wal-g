package internal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/golang/mock/gomock"
	"github.com/wal-g/wal-g/test/mocks"
	mock_internal "github.com/wal-g/wal-g/testtools/mocks"

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

type streamSentinelDto struct {
	StartLocalTime time.Time
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
	}
	return metadata
}

func emptyMetadata() internal.GenericMetadata {
	metadata := internal.GenericMetadata{
		BackupName:       "",
		UncompressedSize: 0,
		CompressedSize:   0,
		Hostname:         "",
		StartTime:        time.Now(),
		FinishTime:       time.Now(),
		IsPermanent:      true,
		UserData:         "",
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

	b := path.Join(utility.BaseBackupPath, "base_123", testBackup.BackupName+".json")
	meta := convertMetadataFetch(testBackup)
	bytesMeta, _ := json.Marshal(&meta)
	_ = folder.PutObject(b, strings.NewReader(string(bytesMeta)))

	backup, err0 := internal.GetBackupByName("base_123", utility.BaseBackupPath, folder)
	assert.NoError(t, err0)

	empMeta := emptyMetadata()
	err := backup.FetchMetadata(&empMeta)
	assert.NoError(t, err)

	assert.Equal(t, testBackup.BackupName, empMeta.BackupName)
	assert.Equal(t, testBackup.UncompressedSize, empMeta.UncompressedSize)
	assert.Equal(t, testBackup.CompressedSize, empMeta.CompressedSize)
	assert.Equal(t, testBackup.StartTime, empMeta.StartTime)
	assert.Equal(t, testBackup.FinishTime, empMeta.FinishTime)
	assert.Equal(t, testBackup.UserData, empMeta.UserData)
	assert.Equal(t, testBackup, empMeta)
}

func TestUploadSentinel(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	folder := mocks.NewMockFolder(mockCtrl)
	defer mockCtrl.Finish()

	uploaderProv := mock_internal.NewMockUploader(mockCtrl)
	uploaderProv.EXPECT().PushStream(gomock.Any(), gomock.Any()).Return("test_file_name", nil)
	uploaderProv.EXPECT().Folder().Return(folder)

	sentinel := streamSentinelDto{StartLocalTime: utility.TimeNowCrossPlatformLocal()}
	fileName, err := uploaderProv.PushStream(context.Background(), bytes.NewReader(getByteSampleArray(51)))
	if err != nil {
		t.Errorf("Error pushing stream: %v", err)
	}
	folder.EXPECT().PutObject(gomock.Any(), gomock.Any()).Return(nil)
	uploadDto := internal.UploadSentinel(uploaderProv, &sentinel, fileName)

	assert.NoError(t, uploadDto)
}
