package internal_test

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/utility"
)

func TestRecoveryMySQLUsesBinlogNamesAndKeepsBoundary(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	base := root.GetSubFolder(utility.BaseBackupPath)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= 8; i++ {
		key := fmt.Sprintf("stream_B%d", i) + utility.SentinelSuffix
		recoveryPut(t, base, key,
			fmt.Sprintf(`{"IsPermanent":false,"IsIncremental":false,"BinLogStart":"mysql-bin.%06d","BinLogFileName":"mysql-bin.%06d"}`, i, i))
		modified := start.Add(time.Duration(i)*time.Hour + time.Minute)
		require.NoError(t, os.Chtimes(root.GetFilePath(path.Join(utility.BaseBackupPath, key)), modified, modified))
	}
	logs := root.GetSubFolder(mysql.BinlogPath)
	for i := 1; i <= 8; i++ {
		key := fmt.Sprintf("mysql-bin.%06d.br", i)
		recoveryPut(t, logs, key, "data")
		modified := start.Add(time.Duration(i) * time.Hour)
		require.NoError(t, os.Chtimes(root.GetFilePath(path.Join(mysql.BinlogPath, key)), modified, modified))
	}
	recoveryPut(t, logs, "binlog_sentinel_005.json", "checkpoint")
	recoveryPut(t, logs, "other-lineage.000001.br", "keep")
	p, err := mysql.PlanRecoveryDeletion(t.Context(), root, "default", []string{"stream_B1", "stream_B2"}, true)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Execute(t.Context(), true))
	recoveryExists(t, logs, "mysql-bin.000002.br", false)
	recoveryExists(t, logs, "mysql-bin.000003.br", true)
	recoveryExists(t, logs, "binlog_sentinel_005.json", true)
	recoveryExists(t, logs, "other-lineage.000001.br", true)
}

func TestRecoveryMySQLKeepsBinlogsAfterNumberingReset(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	base := root.GetSubFolder(utility.BaseBackupPath)
	logs := root.GetSubFolder(mysql.BinlogPath)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	putAt := func(directory, key, content string, hour int) {
		t.Helper()
		recoveryPut(t, root.GetSubFolder(directory), key, content)
		modified := start.Add(time.Duration(hour) * time.Hour)
		require.NoError(t, os.Chtimes(root.GetFilePath(path.Join(directory, key)), modified, modified))
	}

	// Before the reload: 000900 -> 000999 -> 001000, then backup B2.
	// After the reload: 000001 -> 000002 -> 000100, backup B3, then 000101.
	// The new sequence has not reached the old names, so no objects were overwritten.
	// Legacy metadata without ServerUUID must still preserve the newer binlogs.
	for i, number := range []int{1000, 100} {
		name := fmt.Sprintf("stream_B%d", i+2)
		binlog := fmt.Sprintf("mysql-bin.%06d", number)
		putAt(utility.BaseBackupPath, name+utility.SentinelSuffix,
			fmt.Sprintf(`{"IsPermanent":false,"IsIncremental":false,"BinLogStart":%q,"BinLogFileName":%q}`,
				binlog, binlog), 3+i*4)
		putAt(mysql.BinlogPath, binlog+".br", "retained backup start", 2+i*4)
	}
	putAt(utility.BaseBackupPath, "stream_B1/payload", "damaged backup without sentinel", 1)
	candidates := []struct {
		name string
		hour int
		keep bool
	}{
		{"mysql-bin.000900.br", 1, true}, // Old sequence; retained by the number guard.
		{"mysql-bin.000999.br", 2, true}, // Old sequence, at the archive-time boundary.
		{"mysql-bin.000001.br", 4, true}, // First binlog after the reset.
		{"mysql-bin.000002.br", 5, true}, // Next binlog in the same new sequence.
		{"mysql-bin.000101.br", 8, true}, // After the latest retained backup.
	}
	for _, candidate := range candidates {
		putAt(mysql.BinlogPath, candidate.name, "binlog", candidate.hour)
	}

	p, err := mysql.PlanRecoveryDeletion(t.Context(), root, "default", []string{"stream_B1"}, true)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Execute(t.Context(), true))
	recoveryExists(t, base, "stream_B1/payload", false)
	for i, number := range []int{1000, 100} {
		recoveryExists(t, base, fmt.Sprintf("stream_B%d", i+2)+utility.SentinelSuffix, true)
		recoveryExists(t, logs, fmt.Sprintf("mysql-bin.%06d.br", number), true)
	}
	for _, candidate := range candidates {
		t.Run(candidate.name, func(t *testing.T) {
			recoveryExists(t, logs, candidate.name, candidate.keep)
		})
	}
}

func TestRecoveryMongoKeepsBoundaryAndPermanentInterval(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	base := root.GetSubFolder(utility.BaseBackupPath)
	for i := 1; i <= 8; i++ {
		recoveryPut(t, base, fmt.Sprintf("binary_B%d", i)+utility.SentinelSuffix,
			fmt.Sprintf(`{"Permanent":%t,"MongoMeta":{"Before":{"LastMajTS":{"TS":%d,"Inc":0}},"After":{"LastMajTS":{"TS":%d,"Inc":0}}}}`, i == 1, i*100, i*100+10))
	}
	logs := root.GetSubFolder(models.OplogArchBasePath)
	for _, key := range []string{"oplog_90.0_110.0.br", "oplog_200.0_210.0.br", "oplog_290.0_300.0.br", "oplog_300.0_310.0.br", "checkpoint"} {
		recoveryPut(t, logs, key, "data")
	}
	p, err := mongo.PlanRecoveryDeletion(t.Context(), root, "default", []string{"binary_B2"}, true)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.Execute(t.Context(), true))
	recoveryExists(t, logs, "oplog_90.0_110.0.br", true)
	recoveryExists(t, logs, "oplog_200.0_210.0.br", false)
	recoveryExists(t, logs, "oplog_290.0_300.0.br", true)
	recoveryExists(t, logs, "checkpoint", true)
}

func TestRecoveryUnknownBoundaryDeletesBackupButKeepsLogs(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	base := root.GetSubFolder(utility.BaseBackupPath)
	recoveryPut(t, base, "binary_B1/payload", "bad")
	recoveryPut(t, base, "binary_B2"+utility.SentinelSuffix, "bad")
	logs := root.GetSubFolder(models.OplogArchBasePath)
	recoveryPut(t, logs, "oplog_1.0_2.0.br", "data")
	p, err := mongo.PlanRecoveryDeletion(t.Context(), root, "default", []string{"binary_B1"}, true)
	require.NoError(t, err)
	defer p.Close()
	require.ErrorContains(t, p.Execute(t.Context(), true), "unknown recovery boundary")
	recoveryExists(t, base, "binary_B1/payload", false)
	recoveryExists(t, logs, "oplog_1.0_2.0.br", true)
}
