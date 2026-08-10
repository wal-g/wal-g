package mysql

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

const testFilenameSmall = "testdata/binlog_small_test"
const testFilenameBig = "testdata/binlog_big_test"

func TestGetBinlogStartTimestamp(t *testing.T) {
	var tests = []struct {
		name        string
		testLogPath string
		exp         time.Time
	}{
		{"Small instance", testFilenameSmall, time.Unix(int64(1566047760), 0)},
		{"Big real instance", testFilenameBig, time.Unix(int64(1565528401), 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetBinlogStartTimestamp(tt.testLogPath, mysql.MySQLFlavor)
			if err != nil {
				t.Errorf("parseFirstTimestampFromHeader(%s) error %v", tt.testLogPath, err)
			}
			if got != tt.exp {
				t.Errorf("parseFirstTimestampFromHeader(%s) got %v, want %v", tt.testLogPath, got, tt.exp)
			}
		})
	}
}

func TestBinlogNum(t *testing.T) {
	assert.Equal(t, 1, BinlogNum("foo.bar.000001"))
	assert.Equal(t, 1, BinlogNum("foo.bar.01"))
	assert.Equal(t, 1, BinlogNum("foo.bar.1"))
	assert.Equal(t, 1, BinlogNum("foo.000001"))
	assert.Equal(t, 1, BinlogNum("foo.01"))
	assert.Equal(t, 1, BinlogNum("foo.1"))
	assert.Equal(t, 123456, BinlogNum("foo.123456"))
	assert.Equal(t, 123456789, BinlogNum("foo.123456789"))
}

func TestGetBinlogPreviousGTIDsRemoteDoesNotRequireTempDir(t *testing.T) {
	ctx := context.Background()
	binlogData, err := os.ReadFile(testFilenameSmall)
	require.NoError(t, err)
	require.Less(t, len(binlogData), BinlogReadHeaderSize)

	folder := memory.NewFolder("", memory.NewKVS())
	binlogName := filepath.Base(testFilenameSmall)
	require.NoError(t, folder.PutObject(ctx, binlogName, bytes.NewReader(binlogData)))

	expectedGTIDSet, err := GetBinlogPreviousGTIDs(testFilenameSmall, mysql.MySQLFlavor)
	require.NoError(t, err)

	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
	actualGTIDSet, err := GetBinlogPreviousGTIDsRemote(ctx, folder, binlogName, mysql.MySQLFlavor)
	require.NoError(t, err)

	assert.Equal(t, expectedGTIDSet.String(), actualGTIDSet.String())
}
