package mongo_test

import (
	"bytes"
	"encoding/json"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestMongoCopyWithHistoryBuildsContinuousOplogManifest(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	name := "logical_20260721T120000Z"
	sentinel := models.Backup{
		BackupName: name,
		BackupType: common.LogicalBackupType,
		MongoMeta: models.MongoMeta{
			Before: models.NodeMeta{LastMajTS: models.Timestamp{TS: 15, Inc: 1}},
			After:  models.NodeMeta{LastMajTS: models.Timestamp{TS: 25, Inc: 1}},
		},
	}
	data, err := json.Marshal(&sentinel)
	require.NoError(t, err)
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, name, "part.archive.lz4"),
		bytes.NewBufferString("opaque-backup")))
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(name)),
		bytes.NewReader(data)))

	first, err := models.NewArchive(models.Timestamp{TS: 10, Inc: 1}, models.Timestamp{TS: 20, Inc: 1},
		"lz4", models.ArchiveTypeOplog)
	require.NoError(t, err)
	second, err := models.NewArchive(models.Timestamp{TS: 20, Inc: 1}, models.Timestamp{TS: 30, Inc: 1},
		"lz4", models.ArchiveTypeOplog)
	require.NoError(t, err)
	for _, archive := range []models.Archive{first, second} {
		require.NoError(t, from.PutObject(t.Context(), path.Join(models.OplogArchBasePath, archive.Filename()),
			bytes.NewBufferString("opaque-oplog")))
	}

	plan, err := mongo.BuildCopyPlan(t.Context(), from, to, name, true)
	require.NoError(t, err)
	targets := make(map[string]bool)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = true
	}
	require.True(t, targets[path.Join(models.OplogArchBasePath, first.Filename())])
	require.True(t, targets[path.Join(models.OplogArchBasePath, second.Filename())])
}

func TestMongoCopyWithoutHistoryExcludesOplog(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	name := "logical_20260721T120000Z"
	sentinel, err := json.Marshal(&models.Backup{BackupName: name, BackupType: common.LogicalBackupType})
	require.NoError(t, err)
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, name, "part"), bytes.NewBufferString("data")))
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(name)), bytes.NewReader(sentinel)))
	require.NoError(t, from.PutObject(t.Context(), path.Join(models.OplogArchBasePath, "oplog_1.1_2.1.lz4"), bytes.NewBufferString("oplog")))

	plan, err := mongo.BuildCopyPlan(t.Context(), from, to, name, false)
	require.NoError(t, err)
	for _, entry := range plan.Entries() {
		require.NotContains(t, entry.SourcePath, models.OplogArchBasePath)
	}
}
