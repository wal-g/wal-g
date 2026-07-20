package internal_test

import (
	"bytes"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/test/mocks"
	"github.com/wal-g/wal-g/testtools"
	mock_internal "github.com/wal-g/wal-g/testtools/mocks"
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

func CreateMockFolder(ctrl *gomock.Controller) *mocks.MockFolder {
	mockFolder := mocks.NewMockFolder(ctrl)
	subFolder := mocks.NewMockFolder(ctrl)
	mockFolder.EXPECT().GetSubFolder(utility.BaseBackupPath).Return(subFolder).AnyTimes()
	t := time.Now().Truncate(24 * time.Hour)
	mockObject1 := mocks.NewMockObject(ctrl)
	subFolder.EXPECT().Exists(gomock.Any(), "base_123_backup_stop_sentinel.json").Return(true, nil).AnyTimes()
	mockObject1.EXPECT().GetName().Return("base_123_backup_stop_sentinel.json").AnyTimes()
	mockObject1.EXPECT().GetLastModified().Return(t.Add(3 * time.Second)).AnyTimes()
	mockObject2 := mocks.NewMockObject(ctrl)
	subFolder.EXPECT().Exists(gomock.Any(), "base_456_backup_stop_sentinel.json").Return(true, nil).AnyTimes()
	mockObject2.EXPECT().GetName().Return("base_456_backup_stop_sentinel.json").AnyTimes()
	mockObject2.EXPECT().GetLastModified().Return(t.Add(2 * time.Second)).AnyTimes()
	mockObject3 := mocks.NewMockObject(ctrl)
	subFolder.EXPECT().Exists(gomock.Any(), "base_000_backup_stop_sentinel.json").Return(true, nil).AnyTimes()
	mockObject3.EXPECT().GetName().Return("base_000_backup_stop_sentinel.json").AnyTimes()
	mockObject3.EXPECT().GetLastModified().Return(t.Add(3 * time.Second)).AnyTimes()
	// not a sentinel
	mockObject4 := mocks.NewMockObject(ctrl)
	subFolder.EXPECT().Exists(gomock.Any(), "base_123312").Return(true, nil).AnyTimes()
	mockObject4.EXPECT().GetName().Return("base_123312").AnyTimes()
	mockObject4.EXPECT().GetLastModified().Return(t.Add(4 * time.Second)).AnyTimes()
	backupList := []storage.Object{mockObject1, mockObject2, mockObject3, mockObject4}
	subFolder.EXPECT().ListFolder(gomock.Any()).Return(backupList, nil, nil).AnyTimes()
	subFolder.EXPECT().Exists(gomock.Any(), "base_321_backup_stop_sentinel.json").Return(false, nil).AnyTimes()
	return mockFolder
}

func TestGetBackupByName_Latest(t *testing.T) {
	folder := testtools.CreateMockStorageFolder(t.Context())
	backup, err := internal.GetBackupByName(t.Context(), internal.LatestString, utility.BaseBackupPath, folder)
	assert.NoError(t, err)
	assert.Equal(t, folder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_000", backup.Name)
}

func TestGetBackupByName_Latest_WithGomock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFolder := CreateMockFolder(ctrl)
	backup, err := internal.GetBackupByName(t.Context(), internal.LatestString, utility.BaseBackupPath, mockFolder)
	assert.NoError(t, err)
	assert.Equal(t, mockFolder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_000", backup.Name)
}

func TestGetBackupByName_LatestNoBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	folder.PutObject(t.Context(), "folder123/nop", &bytes.Buffer{})
	_, err := internal.GetBackupByName(t.Context(), internal.LatestString, utility.BaseBackupPath, folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewNoBackupsFoundError(), err)
}

func TestGetBackupByName_LatestNoBackups_WithGomock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFolder := mocks.NewMockFolder(ctrl)
	subFolder := mocks.NewMockFolder(ctrl)
	mockFolder.EXPECT().GetSubFolder(utility.BaseBackupPath).Return(subFolder).AnyTimes()
	subFolder.EXPECT().ListFolder(gomock.Any()).Return([]storage.Object{}, nil, nil).AnyTimes()
	_, err := internal.GetBackupByName(t.Context(), internal.LatestString, utility.BaseBackupPath, mockFolder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewNoBackupsFoundError(), err)
}

func TestGetBackupByName_Exists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder(t.Context())
	backup, err := internal.GetBackupByName(t.Context(), "base_123", utility.BaseBackupPath, folder)
	assert.NoError(t, err)
	assert.Equal(t, folder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_123", backup.Name)
}

func TestGetBackupByName_Exists_WithGomock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFolder := CreateMockFolder(ctrl)
	backup, err := internal.GetBackupByName(t.Context(), "base_123", utility.BaseBackupPath, mockFolder)
	assert.NoError(t, err)
	assert.Equal(t, mockFolder.GetSubFolder(utility.BaseBackupPath), backup.Folder)
	assert.Equal(t, "base_123", backup.Name)
}

func TestGetBackupByName_NotExists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder(t.Context())
	_, err := internal.GetBackupByName(t.Context(), "base_321", utility.BaseBackupPath, folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewBackupNonExistenceError(""), err)
}

func TestGetBackupByName_NotExists_WithGomock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockFolder := CreateMockFolder(ctrl)
	_, err := internal.GetBackupByName(t.Context(), "base_321", utility.BaseBackupPath, mockFolder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewBackupNonExistenceError(""), err)
}

func TestFetchMetadata(t *testing.T) {
	folder := testtools.CreateMockStorageFolder(t.Context())

	b := path.Join(utility.BaseBackupPath, "base_123", testBackup.BackupName+".json")
	meta := convertMetadataFetch(testBackup)
	bytesMeta, _ := json.Marshal(&meta)
	_ = folder.PutObject(t.Context(), b, strings.NewReader(string(bytesMeta)))

	backup, err0 := internal.GetBackupByName(t.Context(), "base_123", utility.BaseBackupPath, folder)
	assert.NoError(t, err0)

	empMeta := emptyMetadata()
	err := backup.FetchMetadata(t.Context(), &empMeta)
	assert.NoError(t, err)

	assert.Equal(t, testBackup.BackupName, empMeta.BackupName)
	assert.Equal(t, testBackup.UncompressedSize, empMeta.UncompressedSize)
	assert.Equal(t, testBackup.CompressedSize, empMeta.CompressedSize)
	assert.Equal(t, testBackup.StartTime, empMeta.StartTime)
	assert.Equal(t, testBackup.FinishTime, empMeta.FinishTime)
	assert.Equal(t, testBackup.UserData, empMeta.UserData)
	assert.Equal(t, testBackup, empMeta)
}

func TestFetchSentinel(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_456"
	expected := streamSentinelDto{StartLocalTime: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)}
	data, err := json.Marshal(expected)
	assert.NoError(t, err)

	err = folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, bytes.NewReader(data))
	assert.NoError(t, err)

	backup := internal.Backup{Name: backupName, Folder: folder}

	var actual streamSentinelDto
	err = backup.FetchSentinel(t.Context(), &actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestFetchSentinel_backupFolderIsEmpty(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	backup := internal.Backup{Name: "base_000", Folder: folder}

	var actual streamSentinelDto
	err := backup.FetchSentinel(t.Context(), &actual)

	assert.Error(t, err)
}

func TestFetchSentinel_invalidJSON(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_000"
	err := folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, strings.NewReader("not a json"))
	assert.NoError(t, err)

	backup := internal.Backup{Name: backupName, Folder: folder}
	var actual streamSentinelDto
	err = backup.FetchSentinel(t.Context(), &actual)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch dto from")
}

func TestFetchSentinel_emptyFile(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_000"
	err := folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, &bytes.Buffer{})
	assert.NoError(t, err)

	backup := internal.Backup{Name: backupName, Folder: folder}
	var actual streamSentinelDto
	err = backup.FetchSentinel(t.Context(), &actual)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch dto from")
}

func TestFetchSentinel_extraFieldsInJSON(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_000"
	expected := streamSentinelDto{StartLocalTime: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)}
	raw := map[string]interface{}{
		"StartLocalTime": expected.StartLocalTime,
		"UnknownField":   "ignored",
	}
	data, err := json.Marshal(raw)
	assert.NoError(t, err)
	err = folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, bytes.NewReader(data))
	assert.NoError(t, err)

	backup := internal.Backup{Name: backupName, Folder: folder}
	var actual streamSentinelDto
	err = backup.FetchSentinel(t.Context(), &actual)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestFetchSentinel_missingFieldsInJSON(t *testing.T) {
	type partialSentinelDto struct {
		StartTime time.Time
		Size      int64
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_000"
	err := folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, strings.NewReader(`{"StartTime":"2020-06-01T00:00:00Z"}`))
	assert.NoError(t, err)

	backup := internal.Backup{Name: backupName, Folder: folder}
	var actual partialSentinelDto
	err = backup.FetchSentinel(t.Context(), &actual)

	assert.NoError(t, err)
	assert.Equal(t, time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC), actual.StartTime)
	assert.Equal(t, int64(0), actual.Size)
}

func TestFetchSentinel_sentinelNotFoundForWrongName(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	const backupName = "base_000"
	expected := streamSentinelDto{StartLocalTime: time.Date(2020, 6, 1, 0, 0, 0, 0, time.UTC)}
	data, err := json.Marshal(expected)
	assert.NoError(t, err)

	err = folder.PutObject(t.Context(), backupName+utility.SentinelSuffix, bytes.NewReader(data))
	assert.NoError(t, err)
	wrongBackup := internal.Backup{Name: "base_000_wrong", Folder: folder}
	var actual streamSentinelDto
	err = wrongBackup.FetchSentinel(t.Context(), &actual)
	assert.Error(t, err)
}

func TestUploadSentinel(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	folder := mocks.NewMockFolder(mockCtrl)
	defer mockCtrl.Finish()

	uploaderProv := mock_internal.NewMockUploader(mockCtrl)
	uploaderProv.EXPECT().PushStream(gomock.Any(), gomock.Any()).Return("test_file_name", nil)
	uploaderProv.EXPECT().Folder().Return(folder)

	sentinel := streamSentinelDto{StartLocalTime: utility.TimeNowCrossPlatformLocal()}
	fileName, err := uploaderProv.PushStream(t.Context(), bytes.NewReader(getByteSampleArray(51)))
	if err != nil {
		t.Errorf("Error pushing stream: %v", err)
	}
	folder.EXPECT().PutObject(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	uploadDto := internal.UploadSentinel(t.Context(), uploaderProv, &sentinel, fileName)

	assert.NoError(t, uploadDto)
}
