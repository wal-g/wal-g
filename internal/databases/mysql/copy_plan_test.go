package mysql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func putMySQLBackup(t *testing.T, folder storage.Folder, name string, sentinel mysql.StreamSentinelDto) {
	t.Helper()
	data, err := json.Marshal(&sentinel)
	require.NoError(t, err)
	require.NoError(t, folder.PutObject(t.Context(), path.Join(utility.BaseBackupPath, name, "part.xbstream.lz4"),
		bytes.NewBufferString("payload-"+name)))
	require.NoError(t, folder.PutObject(t.Context(), path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(name)),
		bytes.NewReader(data)))
}

func mysqlCopyFixture(t *testing.T, withBinlogSentinel bool) storage.Folder {
	t.Helper()
	from := testtools.MakeDefaultInMemoryStorageFolder()
	full := "base_full"
	count := 1
	putMySQLBackup(t, from, full, mysql.StreamSentinelDto{BinLogStart: "mysql-bin.000001"})
	putMySQLBackup(t, from, "base_incremental", mysql.StreamSentinelDto{
		BinLogStart:       "mysql-bin.000001",
		IsIncremental:     true,
		IncrementFrom:     &full,
		IncrementFullName: &full,
		IncrementCount:    &count,
	})
	for i := 1; i <= 3; i++ {
		name := path.Join(mysql.BinlogPath, fmt.Sprintf("mysql-bin.%06d.br", i))
		require.NoError(t, from.PutObject(t.Context(), name, bytes.NewBufferString("binlog")))
	}
	if withBinlogSentinel {
		require.NoError(t, from.PutObject(t.Context(), mysql.BinlogSentinelPath,
			bytes.NewBufferString(`{"GtidArchived":"uuid:1-3"}`)))
	}
	return from
}

func TestBuildCopyPlanIncludesMySQLIncrementalAncestorsAndHistory(t *testing.T) {
	from := mysqlCopyFixture(t, true)
	to := testtools.MakeDefaultInMemoryStorageFolder()
	plan, err := mysql.BuildCopyPlan(t.Context(), from, to, "base_incremental", true, "")
	require.NoError(t, err)

	targets := make(map[string]copyutil.Entry)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = entry
	}
	require.Contains(t, targets, path.Join(utility.BaseBackupPath, "base_full", "part.xbstream.lz4"))
	require.Contains(t, targets, path.Join(utility.BaseBackupPath, "base_incremental", "part.xbstream.lz4"))
	require.Contains(t, targets, path.Join(mysql.BinlogPath, "mysql-bin.000001.br"))
	require.Contains(t, targets, path.Join(mysql.BinlogPath, "mysql-bin.000003.br"))
	require.True(t, targets[mysql.BinlogSentinelPath].Mutable)
}

func TestBuildCopyPlanAllowsHistoryWithoutMySQLBinlogSentinel(t *testing.T) {
	from := mysqlCopyFixture(t, false)
	to := testtools.MakeDefaultInMemoryStorageFolder()
	plan, err := mysql.BuildCopyPlan(t.Context(), from, to, "base_incremental", true, "")
	require.NoError(t, err)

	targets := make(map[string]copyutil.Entry)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = entry
	}
	require.Contains(t, targets, path.Join(mysql.BinlogPath, "mysql-bin.000001.br"))
	require.Contains(t, targets, path.Join(mysql.BinlogPath, "mysql-bin.000003.br"))
	require.NotContains(t, targets, mysql.BinlogSentinelPath)
}

func TestBuildCopyPlanSnapshotsMySQLBinlogSentinel(t *testing.T) {
	from := mysqlCopyFixture(t, true)
	to := testtools.MakeDefaultInMemoryStorageFolder()
	plan, err := mysql.BuildCopyPlan(t.Context(), from, to, "base_incremental", true, "")
	require.NoError(t, err)

	require.NoError(t, from.PutObject(t.Context(), path.Join(mysql.BinlogPath, "mysql-bin.000004.br"),
		bytes.NewBufferString("new-binlog")))
	require.NoError(t, from.PutObject(t.Context(), mysql.BinlogSentinelPath,
		bytes.NewBufferString(`{"GtidArchived":"uuid:1-4"}`)))
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))

	var sentinel mysql.BinlogSentinelDto
	require.NoError(t, mysql.FetchBinlogSentinel(t.Context(), to, &sentinel))
	require.Equal(t, "uuid:1-3", sentinel.GTIDArchived)
	exists, err := to.Exists(t.Context(), path.Join(mysql.BinlogPath, "mysql-bin.000004.br"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestLegacyMySQLPrefixRewritesEntireIncrementalChain(t *testing.T) {
	from := mysqlCopyFixture(t, true)
	to := testtools.MakeDefaultInMemoryStorageFolder()
	plan, err := mysql.BuildCopyPlan(t.Context(), from, to, "base_incremental", false, "copied_")
	require.NoError(t, err)
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))

	backup, err := internal.GetBackupByName(t.Context(), "copied_base_incremental", utility.BaseBackupPath, to)
	require.NoError(t, err)
	var sentinel mysql.StreamSentinelDto
	require.NoError(t, backup.FetchSentinel(t.Context(), &sentinel))
	require.Equal(t, "copied_base_full", *sentinel.IncrementFrom)
	require.Equal(t, "copied_base_full", *sentinel.IncrementFullName)
	exists, err := to.Exists(t.Context(), path.Join(utility.BaseBackupPath, "copied_base_full", "part.xbstream.lz4"))
	require.NoError(t, err)
	require.True(t, exists)
}

func TestMySQLCopyAllPreservesThreeLevelIncrementalCommitOrder(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	full := "base_1_full"
	child := "base_2_incremental"
	grandchild := "base_3_incremental"
	putMySQLBackup(t, from, full, mysql.StreamSentinelDto{})
	putMySQLBackup(t, from, child, mysql.StreamSentinelDto{IncrementFrom: &full})
	putMySQLBackup(t, from, grandchild, mysql.StreamSentinelDto{IncrementFrom: &child})

	plan, err := mysql.BuildCopyPlan(t.Context(), from, to, "", false, "")
	require.NoError(t, err)
	phases := make(map[string]uint16)
	for _, entry := range plan.Entries() {
		phases[entry.TargetPath] = uint16(entry.Phase)
	}
	fullSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(full))
	childSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(child))
	grandchildSentinel := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(grandchild))
	require.Less(t, phases[fullSentinel], phases[childSentinel])
	require.Less(t, phases[childSentinel], phases[grandchildSentinel])
}
