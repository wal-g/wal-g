package internal_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/test/mocks"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type TestPostgresBackupObject struct {
	storage.Object
}

func (o TestPostgresBackupObject) IsFullBackup() bool {
	// this function is only valid for test cases,
	// since arbitrary WAL file name may contain the "D" symbol
	return !strings.Contains(o.GetName(), "D")
}

func (o TestPostgresBackupObject) GetBackupTime() time.Time {
	return o.GetLastModified()
}

func TestFindTargetBeforeName_ReturnsBackup_Without_Modifier(t *testing.T) {
	targetDelta := "base_000000010000000000000005_D_000000010000000000000003"
	expected := targetDelta + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.NoDeleteModifier)
}

func TestFindTargetBeforeName_ReturnsForbiddenActionError_With_FULL_Modifier(t *testing.T) {
	folder := createSimpleMockFolderWithoutBackups(t)
	deleteHandler := newTestDeleteHandler(folder, lessByName)

	_, err := deleteHandler.FindTargetBeforeName("", internal.FullDeleteModifier)
	assert.Error(t, err)
	assert.IsType(t, utility.ForbiddenActionError{}, err)
}

func TestFindTargetBeforeName_ReturnsFullBackup_With_FIND_FULL(t *testing.T) {
	targetDelta := "base_000000010000000000000009_D_000000010000000000000007"
	expected := "base_000000010000000000000007" + utility.SentinelSuffix
	testFindTargetBeforeName(t, expected, targetDelta, internal.FindFullDeleteModifier)
}

func testFindTargetBeforeName(t *testing.T, expected, targetName string, modifier int) {
	folder := testtools.CreateMockStorageFolderWithDeltaBackups(t)
	deleteHandler := newTestDeleteHandler(folder, lessByName)
	target, err := deleteHandler.FindTargetBeforeName(targetName, modifier)

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
	mockFolder := createMockFolderWithTime(t, utility.TimeNowCrossPlatformLocal())
	deleteHandler := newTestDeleteHandler(mockFolder, lessByTime)

	target, err := deleteHandler.FindTargetRetain(retentionCount, modifier)

	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func getBoundedValue(leftBound, value, rightBound int) int {
	if value < leftBound {
		return leftBound
	} else if value > rightBound {
		return rightBound
	} else {
		return value
	}
}

func intMin(value1, value2 int) int {
	if value1 > value2 {
		return value2
	} else {
		return value1
	}
}

func TestFindTargetRetainAfter_Without_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 5; retentionCount++ {
		for minutesCount := 0; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(getBoundedValue(0, minutesCount, 4), 5-retentionCount)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.NoDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.NoDeleteModifier)
		}
	}
}

func TestFindTargetRetainAfter_With_FULL_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 3; retentionCount++ {
		for minutesCount := 1; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(((getBoundedValue(0, minutesCount, 4)+1)/2)*2, 6-retentionCount*2)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.FullDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.FullDeleteModifier)
		}
	}
}

func TestFindTargetRetainAfter_With_FIND_FULL_Modifier(t *testing.T) {
	backupNames := []string{
		"base_000000010000000000000000",
		"base_000000010000000000000001_D_000000010000000000000000",
		"base_000000010000000000000002",
		"base_000000010000000000000003_D_000000010000000000000002",
		"base_000000010000000000000004",
	}
	for retentionCount := 1; retentionCount <= 5; retentionCount++ {
		for minutesCount := 0; minutesCount < 5; minutesCount++ {
			expectedIndex := intMin(((getBoundedValue(0, minutesCount, 4)+1)/2)*2, 4-(retentionCount/2)*2)
			expectedName := backupNames[expectedIndex]
			duration := time.Duration(minutesCount * int(time.Minute))
			testTargetRetainAfterTime(t, duration, expectedName, retentionCount, internal.FindFullDeleteModifier)
			testTargetRetainAfterName(t, backupNames[minutesCount], expectedName, retentionCount, internal.FindFullDeleteModifier)
		}
	}
}

func testTargetRetainAfterTime(t *testing.T, duration time.Duration, expectedName string, retentionCount, modifier int) {
	baseTime := utility.TimeNowCrossPlatformLocal()
	mockFolder := createMockFolderWithTime(t, baseTime)
	deleteHandler := newTestDeleteHandler(mockFolder, lessByTime)

	target, err := deleteHandler.FindTargetRetainAfterTime(retentionCount, baseTime.Add(duration), modifier)

	assert.NoError(t, err)
	assert.Equal(t, expectedName, target.GetName())
}

func testTargetRetainAfterName(t *testing.T, name string, expectedName string, retentionCount, modifier int) {
	mockFolder := createMockFolderWithTime(t, utility.TimeNowCrossPlatformLocal())
	deleteHandler := newTestDeleteHandler(mockFolder, lessByTime)

	target, err := deleteHandler.FindTargetRetainAfterName(retentionCount, name, modifier)

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
	baseTime := utility.TimeNowCrossPlatformLocal()
	mockFolder := createMockFolderWithTime(t, baseTime)
	deleteHandler := newTestDeleteHandler(mockFolder, lessByTime)

	timeLine := baseTime.Add(time.Duration(minute * int(time.Minute)))
	return deleteHandler.FindTargetBeforeTime(timeLine, modifier)
}

func verifyThatExistBackupsAndWals(t *testing.T, expectBackupExistAfterDelete, expectWalExistAfterDelete map[string]bool, folder storage.Folder) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	walBackupFolder := folder.GetSubFolder(utility.WalPath)
	for backupName, expect := range expectBackupExistAfterDelete {
		exists, err := baseBackupFolder.Exists(backupName + "/" + utility.MetadataFileName)
		assert.NoError(t, err)
		assert.Equal(t, expect, exists, "errored on "+backupName+"/"+utility.MetadataFileName)
	}
	for walName, expect := range expectWalExistAfterDelete {
		exists, err := walBackupFolder.Exists(walName + ".lz4")
		assert.NoError(t, err)
		assert.Equal(t, expect, exists, "errored on "+walName+".lz4")
	}
}

func TestDeleteBeforeTargetWithPermanentBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolderWithPermanentBackups(t)

	expectBackupExistBeforeDelete := map[string]bool{
		"base_000000010000000000000002":                            true,
		"base_000000010000000000000004_D_000000010000000000000002": true,
		"base_000000010000000000000006_D_000000010000000000000004": true,
	}
	expectWalExistBeforeDelete := map[string]bool{
		"000000010000000000000001": true,
		"000000010000000000000002": true,
		"000000010000000000000003": true,
	}

	expectBackupExistAfterDelete := map[string]bool{
		"base_000000010000000000000002":                            true,
		"base_000000010000000000000004_D_000000010000000000000002": true,
		"base_000000010000000000000006_D_000000010000000000000004": false,
	}
	expectWalExistAfterDelete := map[string]bool{
		"000000010000000000000001": true,
		"000000010000000000000002": true,
		"000000010000000000000003": false,
	}

	// verify that they exist initially
	verifyThatExistBackupsAndWals(t, expectBackupExistBeforeDelete, expectWalExistBeforeDelete, folder)

	// attempt delete
	target := storage.NewLocalObject("", utility.TimeNowCrossPlatformLocal().Add(time.Duration(1*int(time.Minute))), 0)

	permanentBackups, permanentWals := internal.GetPermanentObjects(folder)
	isPermanent := makeTestPermanentFunc(permanentBackups, permanentWals)
	deleteHandler := newTestDeleteHandler(folder, lessByTime, internal.IsPermanentFunc(isPermanent))

	err := deleteHandler.DeleteBeforeTarget(TestPostgresBackupObject{target}, true)
	assert.NoError(t, err)

	// verify expected permanent still exists
	verifyThatExistBackupsAndWals(t, expectBackupExistAfterDelete, expectWalExistAfterDelete, folder)
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
		objects[i] = storage.NewLocalObject(name, baseTime.Add(iDuration), 0)
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

func createSimpleMockFolderWithoutBackups(t *testing.T) *mocks.MockFolder {
	controller := gomock.NewController(t)
	defer controller.Finish()

	objects := make([]storage.Object, 0)
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

func lessByName(object1, object2 storage.Object) bool {
	return object1.GetName() < object2.GetName()
}

func lessByTime(object1, object2 storage.Object) bool {
	return object1.GetLastModified().Before(object2.GetLastModified())
}

func newTestDeleteHandler(
	folder storage.Folder,
	lessFunc func(storage.Object, storage.Object) bool,
	options ...internal.DeleteHandlerOption,
) *internal.DeleteHandler {
	objects, _ := getBackupObjects(folder)

	testBackupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		testBackupObjects = append(testBackupObjects, TestPostgresBackupObject{object})
	}

	return internal.NewDeleteHandler(folder, testBackupObjects, lessFunc, options...)
}

// this function is the analog for internal.GetBackupSentinelObjects
// but we don't use sentinel suffixes in the above tests so there is no sentinel suffix check
func getBackupObjects(folder storage.Folder) ([]storage.Object, error) {
	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func makeTestPermanentFunc(permanentBackups, permanentWals map[string]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		return internal.IsPermanent(object.GetName(), permanentBackups, permanentWals)
	}
}
