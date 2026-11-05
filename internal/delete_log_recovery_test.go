package internal_test

import (
	"fmt"
	"testing"

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
	for i := 1; i <= 8; i++ {
		recoveryPut(t, base, fmt.Sprintf("stream_B%d", i)+utility.SentinelSuffix,
			fmt.Sprintf(`{"IsPermanent":false,"IsIncremental":false,"BinLogStart":"mysql-bin.%06d","BinLogFileName":"mysql-bin.%06d"}`, i, i))
	}
	logs := root.GetSubFolder(mysql.BinlogPath)
	for i := 1; i <= 8; i++ {
		recoveryPut(t, logs, fmt.Sprintf("mysql-bin.%06d.br", i), "data")
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
