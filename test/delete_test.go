package test

import (
	"encoding/json"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/test/mocks"
	"github.com/wal-g/wal-g/testtools"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFindTargetBeforeName_ReturnsBackup_Without_Modifier(t *testing.T) {
	targetDelta := "base_000000010000000000000005_D_000000010000000000000003"
	expected := targetDelta + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.NoDeleteModifier)
}

func TestFindTargetBeforeName_ReturnsForbiddenActionError_With_FULL_Modifier(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	_, err := internal.FindTargetBeforeName(mocks.NewMockFolder(controller), "",
		internal.FullDeleteModifier, isFullBackup, greaterByName)
	assert.Error(t, err)
	assert.IsType(t, utility.ForbiddenActionError{}, err)
}

func TestFindTargetBeforeName_ReturnsFullBackup_With_FIND_FULL(t *testing.T) {
	targetDelta := "base_000000010000000000000009_D_000000010000000000000007"
	expected := "base_000000010000000000000007" + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.FindFullDeleteModifier)
}

func testFindTargetBeforeName(t *testing.T, expected, targetName string, modifier int) {
	folder := createMockStorageFolderWithDeltaBackups(t)
	target, err := internal.FindTargetBeforeName(folder, targetName, modifier, isFullBackup, greaterByName)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func TestFindTargetRetain_Without_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000003_D_000000010000000000000002"
	testTargetRetain(t, expectedName, 2, internal.NoDeleteModifier)
}

func TestFindTargetRetain_With_FULL_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000002"
	testTargetRetain(t, expectedName, 2, internal.FullDeleteModifier)
}

func TestFindTargetRetain_With_FIND_FULL_Modifier(t *testing.T) {
	expectedName := "base_000000010000000000000000"
	testTargetRetain(t, expectedName, 4, internal.FindFullDeleteModifier)
}

func testTargetRetain(t *testing.T, expectedName string, retentionCount, modifier int) {
	mockFolder := createMockFolderWithTime(t, time.Now())

	target, err := internal.FindTargetRetain(mockFolder, retentionCount, modifier, isFullBackup, greaterByTime)
	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func TestFindTargetBeforeTime_ReturnBackup_Without_Modifier(t *testing.T) {
	expected := "base_000000010000000000000001_D_000000010000000000000000"
	target, err := testFindTargetBeforeTime(t, 1, internal.NoDeleteModifier)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func TestFindTargetBeforeTime_ReturnsForbiddenActionError_With_FULL_Modifier(t *testing.T) {
	_, err := testFindTargetBeforeTime(t, 2, internal.FullDeleteModifier)
	assert.Error(t, err)
	assert.IsType(t, utility.ForbiddenActionError{}, err)
}

func TestFindTargetBeforeTime_With_FIND_FULL_Modifier(t *testing.T) {
	expected := "base_000000010000000000000002"
	target, err := testFindTargetBeforeTime(t, 3, internal.FindFullDeleteModifier)
	assert.NoError(t, err)
	assert.Equal(t, expected, target.GetName())
}

func testFindTargetBeforeTime(t *testing.T, minute int, modifier int) (storage.Object, error) {
	baseTime := time.Now()
	mockFolder := createMockFolderWithTime(t, baseTime)

	timeLine := baseTime.Add(time.Duration(minute * int(time.Minute)))
	return internal.FindTargetBeforeTime(mockFolder, timeLine, modifier, isFullBackup, lessByTime)
}

func createMockFolderWithTime(t *testing.T, baseTime time.Time) *mocks.MockFolder {
	baseNamePrefix := "base_"
	deltaMark := "_D_"
	lsnPrefix := "00000001000000000000000"
	objects := make([]storage.Object, 5)
	var lastLSN, name string
	for i := 0; i < 5; i++ {
		iDuration := time.Duration(i * int(time.Minute))
		if i%2 == 0 {
			lastLSN = lsnPrefix + strconv.Itoa(i)
			name = baseNamePrefix + lastLSN
		} else {
			name = baseNamePrefix + lsnPrefix + strconv.Itoa(i) + deltaMark + lastLSN
		}
		objects[i] = storage.NewLocalObject(name, baseTime.Add(iDuration))
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	mockBaseBackupFolder := mocks.NewMockFolder(controller)

	mockBaseBackupFolder.
		EXPECT().
		ListFolder().
		Return(objects, nil, nil).
		AnyTimes()

	mockFolder := mocks.NewMockFolder(controller)

	mockFolder.
		EXPECT().
		GetSubFolder(utility.BaseBackupPath).
		Return(mockBaseBackupFolder).
		AnyTimes()
	return mockFolder
}

func isFullBackup(object storage.Object) bool {
	return !strings.Contains(object.GetName(), "D")
}

func greaterByName(object1, object2 storage.Object) bool {
	return object1.GetName() > object2.GetName()
}

func lessByTime(object1, object2 storage.Object) bool {
	return object1.GetLastModified().Before(object2.GetLastModified())
}

func greaterByTime(object1, object2 storage.Object) bool {
	return object1.GetLastModified().After(object2.GetLastModified())
}

func createMockStorageFolderWithDeltaBackups(t *testing.T) storage.Folder {
	var folder = testtools.MakeDefaultInMemoryStorageFolder()
	subFolder := folder.GetSubFolder(utility.BaseBackupPath)
	sentinelData := map[string]interface{}{
		"DeltaFrom":     "",
		"DeltaFullName": "base_000000010000000000000007",
		"DeltaFromLSN":  0,
		"DeltaCount":    0,
	}
	emptySentinelData := map[string]interface{}{}
	backupNames := map[string]interface{}{
		"base_000000010000000000000003":                            emptySentinelData,
		"base_000000010000000000000005_D_000000010000000000000003": sentinelData,
		"base_000000010000000000000007":                            emptySentinelData,
		"base_000000010000000000000009_D_000000010000000000000007": sentinelData}
	for backupName, sentinelD := range backupNames {
		bytesSentinel, err := json.Marshal(&sentinelD)
		assert.NoError(t, err)
		sentinelString := string(bytesSentinel)
		err = subFolder.PutObject(backupName+utility.SentinelSuffix, strings.NewReader(sentinelString))
		assert.NoError(t, err)
	}
	return folder
}
