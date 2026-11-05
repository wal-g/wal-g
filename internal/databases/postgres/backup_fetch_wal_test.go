package postgres

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/utility"
)

func TestBackupFetchWALUsesPrefetchedSegment(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "pg_wal")
	require.NoError(t, os.Mkdir(walDir, 0700))
	old := viper.Get(conf.PrefetchDir)
	viper.Set(conf.PrefetchDir, t.TempDir())
	t.Cleanup(func() { viper.Set(conf.PrefetchDir, old) })
	name := WalSegmentNo(1).GetFilename(2)
	require.NoError(t, os.WriteFile(filepath.Join(dir, BackupLabelFilename), []byte("START WAL LOCATION: 0/1000000 (file "+name+")\n"), 0600))
	start, end := LSN(WalSegmentSize), LSN(2*WalSegmentSize)
	backup := Backup{Backup: internal.Backup{Name: "base_" + name}, SentinelDto: &BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &end}}
	folder := memory.NewFolder("in_memory/", memory.NewKVS())
	historyName := fmt.Sprintf(walHistoryFileFormat, 2)
	history := []byte("1\t0/1000000\tpromotion\n")
	require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), historyName, bytes.NewReader(history)))
	_, _, _, prefetched := getPrefetchLocations(walDir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(prefetched), 0700))
	data := bytes.Repeat([]byte{42}, int(WalSegmentSize))
	require.NoError(t, os.WriteFile(prefetched, data, 0600))
	// The segment exists only in the prefetch cache; history still comes from storage.
	require.NoError(t, FetchBackupWAL(t.Context(), folder, backup, dir))
	actual, err := os.ReadFile(filepath.Join(walDir, name))
	require.NoError(t, err)
	require.Equal(t, data, actual)
	actual, err = os.ReadFile(filepath.Join(walDir, historyName))
	require.NoError(t, err)
	require.Equal(t, history, actual)
}

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

func TestBackupFetchWALPropagatesFetchErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		timeline    uint32
		badChecksum bool
	}{
		{name: "missing segment", timeline: 1},
		{name: "missing history", timeline: 2},
		{name: "bad segment checksum", timeline: 1, badChecksum: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "pg_wal"), 0700))
			name := WalSegmentNo(1).GetFilename(tc.timeline)
			require.NoError(t, os.WriteFile(filepath.Join(dir, BackupLabelFilename), []byte("START WAL LOCATION: 0/1000000 (file "+name+")\n"), 0600))
			start, end := LSN(WalSegmentSize), LSN(2*WalSegmentSize)
			backup := Backup{Backup: internal.Backup{Name: "base_" + name}, SentinelDto: &BackupSentinelDto{BackupStartLSN: &start, BackupFinishLSN: &end}}
			folder := memory.NewFolder("in_memory/", memory.NewKVS())
			if tc.timeline > 1 {
				// Only history is missing, so skipping its fetch would incorrectly succeed.
				require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), name, bytes.NewReader(bytes.Repeat([]byte{42}, int(WalSegmentSize)))))
			}
			if tc.badChecksum {
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				_, err := writer.Write(bytes.Repeat([]byte{42}, int(WalSegmentSize)))
				require.NoError(t, err)
				require.NoError(t, writer.Close())
				contents := compressed.Bytes()
				contents[len(contents)-8] ^= 1 // bad CRC after a complete decoded segment
				require.NoError(t, folder.GetSubFolder(utility.WalPath).PutObject(t.Context(), name+".gz", bytes.NewReader(contents)))
			}
			// WAL validation and failed-download cleanup belong to HandleWALFetch.
			require.Error(t, FetchBackupWAL(t.Context(), folder, backup, dir))
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
		{name: "missing start", label: label, dto: BackupSentinelDto{BackupFinishLSN: finish}, wantError: "no valid LSN boundaries"},
		{name: "missing finish", label: label, dto: BackupSentinelDto{BackupStartLSN: start}, wantError: "no valid LSN boundaries"},
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
