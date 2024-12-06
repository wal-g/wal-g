package postgres_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestStartCopy_WhenThereAreNoObjectsToCopy(t *testing.T) {
	var infos = make([]copy.InfoProvider, 0)
	var err = copy.Infos(infos)
	assert.NoError(t, err)
}

func TestStartCopy_WhenThereAreObjectsToCopy(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	infos, err := postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	err = copy.Infos(infos)
	assert.NoError(t, err)

	for _, info := range infos {
		var result, err = to.Exists(info.SrcObj.GetName())
		assert.NoError(t, err)
		if !result {
			tracelog.InfoLogger.Println("Filename '" + info.SrcObj.GetName() + "' not found")
		}
		assert.True(t, result)
	}
}

func TestGetBackupCopyingInfo_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000002")
	assert.NoError(t, err)
	infos, err := postgres.BackupCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetBackupCopyingInfo_WhenFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000002")
	assert.NoError(t, err)
	infos, err := postgres.BackupCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos))
	assert.NotEmpty(t, infos)
}

func TestGetHistoryCopyingInfo_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000002")
	assert.NoError(t, err)
	infos, err := postgres.HistoryCopyingInfo(backup, from, to, true)
	assert.Error(t, err)
	assert.Empty(t, infos)
}

func TestGetHistoryCopyingInfo_WhenThereIsNoHistoryObjects(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000002")
	assert.NoError(t, err)
	metadata := map[string]interface{}{"finish_lsn": postgres.WalSegmentSize * 4, "start_lsn": postgres.WalSegmentSize * 2}
	bytesMetadata, err := json.Marshal(&metadata)
	assert.NoError(t, err)
	from.PutObject("base_000000010000000000000002/"+utility.MetadataFileName, strings.NewReader(string(bytesMetadata)))
	infos, err := postgres.HistoryCopyingInfo(backup, from, to, true)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetHistoryCopyingInfo_WithAllHistory(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000004")
	metadata := map[string]interface{}{"finish_lsn": postgres.WalSegmentSize * 4, "start_lsn": postgres.WalSegmentSize * 2}
	bytesMetadata, err := json.Marshal(&metadata)
	assert.NoError(t, err)
	from.PutObject("base_000000010000000000000004/"+utility.MetadataFileName, strings.NewReader(string(bytesMetadata)))
	subFolderWals := from.GetSubFolder(utility.WalPath)
	subFolderWals.PutObject("000000010000000000000000", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000001", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000002", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000003", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000004.00000028.br", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000004", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000005", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000006", &bytes.Buffer{})         //nolint:errcheck
	assert.NoError(t, err)
	infos, err := postgres.HistoryCopyingInfo(backup, from, to, true)
	assert.NoError(t, err)
	// from 1 to 6 (with walg backup info file)
	assert.Equal(t, 7, len(infos))
	assert.NotEmpty(t, infos)
}

func TestGetHistoryCopyingInfo_WithoutAllHistory(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup, err = postgres.NewBackup(from, "base_000000010000000000000004")
	metadata := map[string]interface{}{"finish_lsn": postgres.WalSegmentSize * 4, "start_lsn": postgres.WalSegmentSize * 2}
	bytesMetadata, err := json.Marshal(&metadata)
	assert.NoError(t, err)
	from.PutObject("base_000000010000000000000004/"+utility.MetadataFileName, strings.NewReader(string(bytesMetadata)))
	subFolderWals := from.GetSubFolder(utility.WalPath)
	subFolderWals.PutObject("000000010000000000000000", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000001", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000002", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000003", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000004.00000028.br", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000004", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000005", &bytes.Buffer{})         //nolint:errcheck
	subFolderWals.PutObject("000000010000000000000006", &bytes.Buffer{})         //nolint:errcheck
	assert.NoError(t, err)
	infos, err := postgres.HistoryCopyingInfo(backup, from, to, false)
	assert.NoError(t, err)
	// from 1 to 4 (with walg backup info file)
	assert.Equal(t, 5, len(infos))
	assert.NotEmpty(t, infos)
}

func TestGetAllCopyingInfo_WhenFromFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetAllCopyingInfo_WhenFromFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	assert.NotEmpty(t, infos)

	for _, info := range infos {
		var result, err = from.Exists(info.SrcObj.GetName())
		assert.NoError(t, err)
		assert.True(t, result)
	}
}

func TestBuildCopyingInfos_WhenThereNoObjectsInFolder(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()

	var infos = copy.BuildCopyingInfos(
		from,
		to,
		make([]storage.Object, 0),
		func(object storage.Object) bool { return true },
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	)
	assert.Empty(t, infos)
}

func TestBuildCopyingInfos_WhenConditionIsJustFalse(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	objects, err := storage.ListFolderRecursively(from)
	assert.NoError(t, err)
	var infos = copy.BuildCopyingInfos(
		from,
		to,
		objects,
		func(object storage.Object) bool { return false },
		copy.NoopRenameFunc,
		copy.NoopSourceTransformer,
	)
	assert.Empty(t, infos)
}

func TestBuildCopyingInfos_WhenComplexCondition(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()

	objects, err := storage.ListFolderRecursively(from)
	assert.NoError(t, err)
	var condition = func(object storage.Object) bool { return strings.HasSuffix(object.GetName(), ".json") }
	var expectedCount int
	for _, object := range objects {
		if condition(object) {
			expectedCount += 1
		}
	}

	assert.NotZero(t, expectedCount)

	var infos = copy.BuildCopyingInfos(from, to, objects, condition, copy.NoopRenameFunc, copy.NoopSourceTransformer)
	assert.Equal(t, expectedCount, len(infos))
	for _, info := range infos {
		assert.True(t, condition(info.SrcObj))
	}
}
