package internal

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func CreateMockStorageFolder(ctx context.Context) storage.Folder {
	var folder = memory.NewFolder("in_memory/", memory.NewKVS())
	subFolder := folder.GetSubFolder("basebackups_005/")
	subFolder.PutObject(ctx, "base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	subFolder.PutObject(ctx, "base_456_backup_stop_sentinel.json", strings.NewReader("{}"))
	subFolder.PutObject(ctx, "base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	subFolder.PutObject(ctx, "base_123312", &bytes.Buffer{})                        // not a sentinel
	subFolder.PutObject(ctx, "base_321/nop", &bytes.Buffer{})
	subFolder.PutObject(ctx, "folder123/nop", &bytes.Buffer{})
	subFolder.PutObject(ctx, "base_456/tar_partitions/1", &bytes.Buffer{})
	subFolder.PutObject(ctx, "base_456/tar_partitions/2", &bytes.Buffer{})
	subFolder.PutObject(ctx, "base_456/tar_partitions/3", &bytes.Buffer{})
	subFolder.PutObject(ctx, "base_456/some_folder/3", &bytes.Buffer{})
	return folder
}

func CreateMockDeleteHandler(backups []BackupObject, folder storage.Folder) *DeleteHandler {
	lessFunction := func(object1, object2 storage.Object) bool { return object1.GetName() < object2.GetName() }
	deleteHandler := NewDeleteHandler(folder, backups, lessFunction)
	return deleteHandler
}

func TestDeleteOldObjects(t *testing.T) {
	folder := CreateMockStorageFolder(t.Context())
	expectedOnlyOneSavedObjectName := "basebackups_005/base_123312"
	filter := func(object storage.Object) bool {
		return object.GetName() != expectedOnlyOneSavedObjectName
	}

	folderFilter := func(path string) bool { return true }
	err := DeleteObjectsWhere(t.Context(), folder, true, filter, folderFilter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(t.Context(), folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}

func TestDeleteOldObjectsWithFilter(t *testing.T) {
	folder := CreateMockStorageFolder(t.Context())
	expectedOnlyOneSavedObjectName := "basebackups_005/base_456/some_folder/3"
	filter := func(object storage.Object) bool {
		return true
	}

	folderFilter := func(name string) bool {
		return !strings.HasPrefix(name, "basebackups_005/base_456/some_folder")
	}

	err := DeleteObjectsWhere(t.Context(), folder, true, filter, folderFilter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(t.Context(), folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}

func TestFindTargetByName(t *testing.T) {
	mockFolder := CreateMockStorageFolder(t.Context())
	objects, _, _ := mockFolder.GetSubFolder("basebackups_005/").ListFolder(t.Context())
	backupObjects := []BackupObject{NewDefaultBackupObject(objects[0])}
	deleteHandler := CreateMockDeleteHandler(backupObjects, mockFolder)

	testCases := []struct {
		target   string
		expected string
	}{
		{
			"",
			backupObjects[0].GetName(),
		},
		{
			backupObjects[0].GetName(),
			backupObjects[0].GetName(),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			actual, err := deleteHandler.FindTargetByName(tc.target)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual.GetName())
		})
	}
}

func TestFindTargetByNameNotContains(t *testing.T) {
	mockFolder := CreateMockStorageFolder(t.Context())
	objects, _, _ := mockFolder.GetSubFolder("basebackups_005/").ListFolder(t.Context())
	backupObjects := []BackupObject{NewDefaultBackupObject(objects[0])}
	deleteHandler := CreateMockDeleteHandler(backupObjects, mockFolder)

	notExistTarget := "base_567_backup_stop_sentinel.json"
	actual, err := deleteHandler.FindTargetByName(notExistTarget)
	assert.Error(t, err)
	assert.Equal(t, nil, actual)
}

func TestFindTargetByNameEmpty(t *testing.T) {
	mockFolder := CreateMockStorageFolder(t.Context())
	deleteHandler := CreateMockDeleteHandler([]BackupObject{}, mockFolder)
	actual, err := deleteHandler.FindTargetByName("base_123312")
	assert.Error(t, err)
	assert.Equal(t, nil, actual)
}

type testBackupObject struct {
	DefaultBackupObject
	isFull bool
}

func (o testBackupObject) IsFullBackup() bool {
	return o.isFull
}

var deleteBeforeBaseTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestBackupObject(name string, hours int, isFull bool) BackupObject {
	object := storage.NewLocalObject(name, deleteBeforeBaseTime.Add(time.Duration(hours)*time.Hour), 0)
	return testBackupObject{DefaultBackupObject{object}, isFull}
}

func createDeleteBeforeHandler() *DeleteHandler {
	backups := []BackupObject{
		newTestBackupObject("base_003", 3, true),
		newTestBackupObject("base_001", 1, true),
		newTestBackupObject("base_004_D_003", 4, false),
		newTestBackupObject("base_002_D_001", 2, false),
	}
	return CreateMockDeleteHandler(backups, nil)
}

func TestFindTargetBefore(t *testing.T) {
	atTime := func(minutes int, zone *time.Location) string {
		return deleteBeforeBaseTime.Add(time.Duration(minutes) * time.Minute).In(zone).Format(time.RFC3339)
	}
	moscow := time.FixedZone("MSK", 3*60*60)

	testCases := []struct {
		name      string
		beforeStr string
		modifier  int
		expected  string
	}{
		{"name of full backup", "base_003", NoDeleteModifier, "base_003"},
		{"name of delta backup", "base_002_D_001", NoDeleteModifier, "base_002_D_001"},
		{"prefix matching several backups", "base_00", NoDeleteModifier, "base_004_D_003"},
		{"name of full backup with FIND_FULL", "base_003", FindFullDeleteModifier, "base_003"},
		{"name of delta backup with FIND_FULL", "base_004_D_003", FindFullDeleteModifier, "base_003"},
		{"time of full backup", atTime(180, time.UTC), NoDeleteModifier, "base_003"},
		{"time of delta backup", atTime(240, time.UTC), NoDeleteModifier, "base_004_D_003"},
		{"time between backups", atTime(150, time.UTC), NoDeleteModifier, "base_003"},
		{"time before all backups", atTime(0, time.UTC), NoDeleteModifier, "base_001"},
		{"time in another time zone", atTime(180, moscow), NoDeleteModifier, "base_003"},
		{"time of full backup with FIND_FULL", atTime(180, time.UTC), FindFullDeleteModifier, "base_003"},
		{"time of delta backup with FIND_FULL", atTime(120, time.UTC), FindFullDeleteModifier, "base_001"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target, err := createDeleteBeforeHandler().FindTargetBefore(tc.beforeStr, tc.modifier)
			assert.NoError(t, err)
			require.NotNil(t, target)
			assert.Equal(t, tc.expected, target.GetName())
		})
	}
}

func TestFindTargetBeforeNotFound(t *testing.T) {
	testCases := []struct {
		name      string
		beforeStr string
		modifier  int
	}{
		{"unknown name", "base_999", NoDeleteModifier},
		{"unknown name with FIND_FULL", "base_999", FindFullDeleteModifier},
		{"date which is not RFC3339 is treated as a name", "2024-01-01", NoDeleteModifier},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target, err := createDeleteBeforeHandler().FindTargetBefore(tc.beforeStr, tc.modifier)
			assert.ErrorIs(t, err, errNotFound)
			assert.Nil(t, target)
		})
	}
}

func TestFindTargetBeforeDeltaWithoutFullBackup(t *testing.T) {
	backups := []BackupObject{
		newTestBackupObject("base_002_D_001", 2, false),
		newTestBackupObject("base_003", 3, true),
	}
	handler := CreateMockDeleteHandler(backups, nil)

	target, err := handler.FindTargetBefore("base_002_D_001", FindFullDeleteModifier)
	assert.ErrorIs(t, err, errNotFound)
	assert.Nil(t, target)
}

func TestFindTargetBeforeTimeAfterAllBackups(t *testing.T) {
	after := deleteBeforeBaseTime.Add(5 * time.Hour).Format(time.RFC3339)

	target, err := createDeleteBeforeHandler().FindTargetBefore(after, NoDeleteModifier)
	assert.NoError(t, err)
	assert.Nil(t, target)
}

func TestFindTargetBeforeNoBackups(t *testing.T) {
	handler := CreateMockDeleteHandler([]BackupObject{}, nil)

	target, err := handler.FindTargetBefore(deleteBeforeBaseTime.Format(time.RFC3339), NoDeleteModifier)
	assert.NoError(t, err)
	assert.Nil(t, target)
}

func TestFindTargetBeforeForbiddenModifier(t *testing.T) {
	testCases := []struct {
		name      string
		beforeStr string
	}{
		{"by name", "base_003"},
		{"by time", deleteBeforeBaseTime.Format(time.RFC3339)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			target, err := createDeleteBeforeHandler().FindTargetBefore(tc.beforeStr, FullDeleteModifier)
			assert.ErrorAs(t, err, &utility.ForbiddenActionError{})
			assert.Nil(t, target)
		})
	}
}
