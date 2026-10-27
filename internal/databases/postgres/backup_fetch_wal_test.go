package postgres_test

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupFetchWAL(t *testing.T) {
	for _, walDir := range []string{"pg_wal", "pg_xlog"} {
		t.Run(walDir, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, walDir), 0700))
			start := postgres.WalSegmentNo(255).GetFilename(1)
			require.NoError(t, os.WriteFile(filepath.Join(dir, postgres.BackupLabelFilename), []byte(fmt.Sprintf("START WAL LOCATION: 0/FF000028 (file %s)\n", start)), 0600))
			end := postgres.LSN(257 * postgres.WalSegmentSize) // exclusive: do not request segment 257
			backup := postgres.Backup{SentinelDto: &postgres.BackupSentinelDto{BackupFinishLSN: &end}}
			folder := testtools.MakeDefaultInMemoryStorageFolder()
			data := bytes.Repeat([]byte{42}, int(postgres.WalSegmentSize))
			for _, seg := range []uint64{255, 256} {
				require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), postgres.WalSegmentNo(seg).GetFilename(1), bytes.NewReader(data)))
			}
			require.NoError(t, postgres.FetchBackupWAL(t.Context(), folder, backup, dir))
			for _, seg := range []uint64{255, 256} {
				actual, err := os.ReadFile(filepath.Join(dir, walDir, postgres.WalSegmentNo(seg).GetFilename(1)))
				require.NoError(t, err)
				require.Equal(t, data, actual)
			}
		})
	}
}

func TestBackupFetchWALFailureDoesNotPublish(t *testing.T) {
	for _, contents := range [][]byte{nil, []byte("truncated WAL"), bytes.Repeat([]byte{42}, int(postgres.WalSegmentSize))} {
		t.Run(fmt.Sprintf("size_%d", len(contents)), func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "pg_wal"), 0700))
			name := postgres.WalSegmentNo(1).GetFilename(1)
			require.NoError(t, os.WriteFile(filepath.Join(dir, postgres.BackupLabelFilename), []byte(fmt.Sprintf("START WAL LOCATION: 0/1000028 (file %s)\n", name)), 0600))
			backup := postgres.Backup{SentinelDto: &postgres.BackupSentinelDto{EndWalFileName: name}}
			folder := testtools.MakeDefaultInMemoryStorageFolder()
			if contents != nil {
				objectName := name
				if len(contents) == int(postgres.WalSegmentSize) {
					var compressed bytes.Buffer
					writer := gzip.NewWriter(&compressed)
					_, err := writer.Write(contents)
					require.NoError(t, err)
					require.NoError(t, writer.Close())
					contents = compressed.Bytes()
					contents[len(contents)-8] ^= 1 // bad CRC after a complete decoded segment
					objectName += ".gz"
				}
				require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), objectName, bytes.NewReader(contents)))
			}
			err := postgres.FetchBackupWAL(t.Context(), folder, backup, dir)
			require.Error(t, err)
			_, err = os.Stat(filepath.Join(dir, "pg_wal", name))
			require.True(t, os.IsNotExist(err))
			entries, err := os.ReadDir(filepath.Join(dir, "pg_wal"))
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestBackupWALRange(t *testing.T) {
	finish := LSN(4 * WalSegmentSize)
	for _, tc := range []struct {
		name      string
		start     string
		dto       BackupSentinelDto
		wantError string
	}{
		{name: "legacy exclusive end", start: WalSegmentNo(2).GetFilename(3), dto: BackupSentinelDto{BackupFinishLSN: &finish}},
		{name: "explicit filenames", start: WalSegmentNo(2).GetFilename(3), dto: BackupSentinelDto{EndWalFileName: WalSegmentNo(3).GetFilename(3)}},
		{name: "label overrides sentinel start", start: WalSegmentNo(2).GetFilename(3), dto: BackupSentinelDto{StartWalFileName: WalSegmentNo(1).GetFilename(3), EndWalFileName: WalSegmentNo(3).GetFilename(3)}},
		{name: "missing finish", start: WalSegmentNo(2).GetFilename(3), wantError: "no WAL end"},
		{name: "reversed", start: WalSegmentNo(5).GetFilename(3), dto: BackupSentinelDto{BackupFinishLSN: &finish}, wantError: "ends before"},
		{name: "timeline mismatch", start: WalSegmentNo(2).GetFilename(3), dto: BackupSentinelDto{EndWalFileName: WalSegmentNo(3).GetFilename(4)}, wantError: "invalid backup end"},
		{name: "malformed label", start: "../../invalid", dto: BackupSentinelDto{BackupFinishLSN: &finish}, wantError: "no valid START"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			label := []byte(fmt.Sprintf("START WAL LOCATION: 0/2000000 (file %s)\n", tc.start))
			timeline, first, last, err := backupWALRange(tc.dto, label)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint32(3), timeline)
			require.Equal(t, WalSegmentNo(2), first)
			require.Equal(t, WalSegmentNo(3), last)
		})
	}
}
