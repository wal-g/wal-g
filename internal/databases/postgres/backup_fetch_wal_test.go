package postgres

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupFetchWAL(t *testing.T) {
	for _, walDir := range []string{"pg_wal", "pg_xlog"} {
		t.Run(walDir, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, walDir), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, BackupLabelFilename), []byte("START WAL LOCATION: 0/FF000000 (file "+WalSegmentNo(255).GetFilename(1)+")\n"), 0600))
			start := LSN(255 * WalSegmentSize)
			end := LSN(257 * WalSegmentSize) // exclusive: do not request segment 257
			backup := Backup{Backup: internal.Backup{Name: "base_" + WalSegmentNo(254).GetFilename(9)}, SentinelDto: &BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &end}}
			folder := memory.NewFolder("in_memory/", memory.NewKVS())
			data := bytes.Repeat([]byte{42}, int(WalSegmentSize))
			for _, seg := range []uint64{255, 256} {
				require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), WalSegmentNo(seg).GetFilename(1), bytes.NewReader(data)))
			}
			require.NoError(t, FetchBackupWAL(t.Context(), folder, backup, dir))
			for _, seg := range []uint64{255, 256} {
				actual, err := os.ReadFile(filepath.Join(dir, walDir, WalSegmentNo(seg).GetFilename(1)))
				require.NoError(t, err)
				require.Equal(t, data, actual)
			}
		})
	}
}

func TestBackupFetchWALFailureDoesNotPublish(t *testing.T) {
	for _, contents := range [][]byte{nil, []byte("truncated WAL"), bytes.Repeat([]byte{42}, int(WalSegmentSize))} {
		t.Run(fmt.Sprintf("size_%d", len(contents)), func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "pg_wal"), 0700))
			require.NoError(t, os.WriteFile(filepath.Join(dir, BackupLabelFilename), []byte("START WAL LOCATION: 0/1000000 (file "+WalSegmentNo(1).GetFilename(1)+")\n"), 0600))
			name := WalSegmentNo(1).GetFilename(1)
			start, end := LSN(WalSegmentSize), LSN(2*WalSegmentSize)
			backup := Backup{Backup: internal.Backup{Name: "base_" + name}, SentinelDto: &BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &end}}
			folder := memory.NewFolder("in_memory/", memory.NewKVS())
			if contents != nil {
				objectName := name
				if len(contents) == int(WalSegmentSize) {
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
			err := FetchBackupWAL(t.Context(), folder, backup, dir)
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
	originalSize := WalSegmentSize
	t.Cleanup(func() { WalSegmentSize = originalSize })
	for _, size := range []uint64{16 << 20, 64 << 20} {
		t.Run(fmt.Sprintf("segment_size_%d", size), func(t *testing.T) {
			WalSegmentSize = size
			testBackupWALRange(t)
		})
	}
}

func testBackupWALRange(t *testing.T) {
	lsn := func(value uint64) *LSN { result := LSN(value); return &result }
	start, finish := lsn(2*WalSegmentSize), lsn(4*WalSegmentSize)
	label := "START WAL LOCATION: 0/1000028 (file " + WalSegmentNo(1).GetFilename(3) + ")\n"
	for _, tc := range []struct {
		name        string
		label       string
		dto         BackupSentinelDto
		first, last WalSegmentNo
		wantError   string
	}{
		{name: "exact segment boundaries", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: finish}, first: 2, last: 3},
		{name: "inside segments", label: label, dto: BackupSentinelDto{BackupStartLSN: lsn(2*WalSegmentSize + 40), BackupFinishLSN: lsn(3*WalSegmentSize + 40)}, first: 2, last: 3},
		{name: "single segment", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: lsn(2*WalSegmentSize + 40)}, first: 2, last: 2},
		{name: "delta uses selected backup", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: finish, IncrementFromLSN: lsn(1)}, first: 2, last: 3},
		{name: "LSNs override filenames", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: finish, StartWalFileName: WalSegmentNo(1).GetFilename(1), EndWalFileName: WalSegmentNo(8).GetFilename(4)}, first: 2, last: 3},
		{name: "missing start", label: label, dto: BackupSentinelDto{BackupFinishLSN: finish}, wantError: "no valid start LSN"},
		{name: "zero start", label: label, dto: BackupSentinelDto{BackupStartLSN: lsn(0), BackupFinishLSN: finish}, wantError: "no valid start LSN"},
		{name: "missing finish", label: label, dto: BackupSentinelDto{BackupStartLSN: start}, wantError: "no valid finish LSN"},
		{name: "zero finish", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: lsn(0)}, wantError: "no valid finish LSN"},
		{name: "reversed within segment", label: label, dto: BackupSentinelDto{BackupStartLSN: lsn(2*WalSegmentSize + 40), BackupFinishLSN: start}, wantError: "ends before"},
		{name: "equal LSNs", label: label, dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: start}, wantError: "ends before"},
		{name: "CRLF label", label: "START WAL LOCATION: 0/1000028 (file " + WalSegmentNo(1).GetFilename(3) + ")\r\n", dto: BackupSentinelDto{BackupStartLSN: start, BackupFinishLSN: finish}, first: 2, last: 3},
		{name: "empty label", wantError: "no valid START WAL LOCATION"},
		{name: "malformed filename", label: "START WAL LOCATION: 0/1000028 (file ../../invalid)\n", wantError: "no valid START WAL LOCATION"},
		{name: "zero timeline", label: "START WAL LOCATION: 0/1000028 (file 000000000000000000000001)\n", wantError: "invalid backup start WAL filename"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			timeline, first, last, err := backupWALRange(tc.dto, []byte(tc.label))
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint32(3), timeline)
			require.Equal(t, tc.first, first)
			require.Equal(t, tc.last, last)
		})
	}
}
