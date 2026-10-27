package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realBinlogListJSON and realBackupListJSON are the outputs of 'wal-g binlog-list --json' and
// 'wal-g backup-list --detail --json' of the MySQL binary.
const realBinlogListJSON = `[{"name":"mysql-bin.000001","size":1000,"last_modified":"2026-10-27T17:50:23.625471526+01:00"},{"name":"mysql-bin.000002","size":2000,"last_modified":"2026-10-27T17:50:23.631783551+01:00"}]`

const realBackupListJSON = `[{"backup_name":"stream_20260101T000000Z","modify_time":"2026-10-27T17:50:53.340358836+01:00","binlog_start":"mysql-bin.000001","binlog_end":"mysql-bin.000002","start_local_time":"2026-01-01T00:00:00Z","stop_local_time":"2026-01-01T00:05:00Z","uncompressed_size":5000,"compressed_size":1000,"hostname":"db1","is_permanent":false}]`

func TestParseBinlogList(t *testing.T) {
	t.Run("valid output", func(t *testing.T) {
		binlogs, err := parseBinlogList([]byte(realBinlogListJSON))
		require.NoError(t, err)
		require.Len(t, binlogs, 2)

		assert.Equal(t, "mysql-bin.000001", binlogs[0].Name)
		assert.Equal(t, int64(1000), binlogs[0].Size)
		assert.Equal(t, "mysql-bin.000002", binlogs[1].Name)
		assert.Equal(t, int64(2000), binlogs[1].Size)
		assert.Equal(t, 2026, binlogs[1].ModifiedTime.Year())
	})

	t.Run("empty output", func(t *testing.T) {
		binlogs, err := parseBinlogList(nil)
		require.NoError(t, err)
		assert.Empty(t, binlogs)
	})

	t.Run("empty list", func(t *testing.T) {
		binlogs, err := parseBinlogList([]byte("[]"))
		require.NoError(t, err)
		assert.Empty(t, binlogs)
	})

	t.Run("invalid json", func(t *testing.T) {
		_, err := parseBinlogList([]byte("name size modified"))
		require.Error(t, err)
	})
}

// writeFakeWalg writes a 'wal-g' stand-in that prints the given outputs for backup-list and binlog-list
// and fails for anything else, and returns its path.
func writeFakeWalg(t *testing.T, backupList, binlogList string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "backups.json"), []byte(backupList), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "binlogs.json"), []byte(binlogList), 0o600))

	script := "#!/bin/sh\n" +
		"case \"$1 $2 $3\" in\n" +
		"  \"backup-list --detail --json\") cat " + filepath.Join(dir, "backups.json") + " ;;\n" +
		"  \"binlog-list --json\"*) cat " + filepath.Join(dir, "binlogs.json") + " ;;\n" +
		"  *) echo \"unexpected arguments: $*\" >&2; exit 1 ;;\n" +
		"esac\n"
	path := filepath.Join(dir, "wal-g")
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
	return path
}

func TestFetchUsesMachineReadableOutput(t *testing.T) {
	e := &Exporter{walgPath: writeFakeWalg(t, realBackupListJSON, realBinlogListJSON)}

	backups, err := e.fetchBackupList()
	require.NoError(t, err)
	require.Len(t, backups, 1)
	assert.Equal(t, "stream_20260101T000000Z", backups[0].BackupName)
	assert.Equal(t, "mysql-bin.000002", backups[0].BinLogEnd)
	assert.Equal(t, int64(1000), backups[0].CompressedSize)

	binlogs, err := e.fetchBinlogList()
	require.NoError(t, err)
	require.Len(t, binlogs, 2)
	assert.Equal(t, "mysql-bin.000001", binlogs[0].Name)
}

func TestFetchBinlogListNoBinlogs(t *testing.T) {
	// 'wal-g binlog-list' prints nothing and exits with 0 when the storage has no binlogs.
	e := &Exporter{walgPath: writeFakeWalg(t, "[]", "")}

	binlogs, err := e.fetchBinlogList()
	require.NoError(t, err)
	assert.Empty(t, binlogs)
}

func TestFetchBinlogListCommandFailure(t *testing.T) {
	// a failing command must not look like an empty storage
	e := &Exporter{walgPath: "/nonexistent/wal-g"}

	_, err := e.fetchBinlogList()
	require.Error(t, err)
}

func TestBackupDuration(t *testing.T) {
	t.Run("valid times", func(t *testing.T) {
		start := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
		stop := start.Add(30 * time.Minute)
		b := &BackupInfo{StartLocalTime: start, StopLocalTime: stop}
		assert.Equal(t, (30 * time.Minute).Seconds(), b.duration())
	})

	t.Run("zero times returns 0", func(t *testing.T) {
		b := &BackupInfo{}
		assert.Equal(t, float64(0), b.duration())
	})
}

func TestOldestNonPermanentStart(t *testing.T) {
	t1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC)

	t.Run("returns oldest non-permanent", func(t *testing.T) {
		backups := []BackupInfo{
			{StartLocalTime: t1, IsPermanent: false},
			{StartLocalTime: t2, IsPermanent: false},
			{StartLocalTime: t3, IsPermanent: true},
		}
		result := oldestNonPermanentStart(backups)
		assert.Equal(t, t1, result)
	})

	t.Run("all permanent returns zero time", func(t *testing.T) {
		backups := []BackupInfo{
			{StartLocalTime: t1, IsPermanent: true},
		}
		result := oldestNonPermanentStart(backups)
		assert.True(t, result.IsZero())
	})

	t.Run("empty slice returns zero time", func(t *testing.T) {
		result := oldestNonPermanentStart(nil)
		assert.True(t, result.IsZero())
	})
}

func TestLatestBinlogTime(t *testing.T) {
	t1 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)

	t.Run("returns latest", func(t *testing.T) {
		binlogs := []BinlogInfo{
			{ModifiedTime: t1},
			{ModifiedTime: t2},
			{ModifiedTime: t3},
		}
		assert.Equal(t, t2, latestBinlogTime(binlogs))
	})

	t.Run("empty slice returns zero time", func(t *testing.T) {
		assert.True(t, latestBinlogTime(nil).IsZero())
	})
}

func TestUpdateBinlogCoverage(t *testing.T) {
	t.Run("computes coverage correctly", func(t *testing.T) {
		e := &Exporter{
			lastOldestBackupStart: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			lastLatestBinlogTime:  time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
			binlogCoverage:        newTestGauge(),
		}
		e.updateBinlogCoverage()
		assert.Equal(t, (24 * time.Hour).Seconds(), gaugeValue(e.binlogCoverage))
	})

	t.Run("zero when binlog time is zero", func(t *testing.T) {
		e := &Exporter{
			lastOldestBackupStart: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			binlogCoverage:        newTestGauge(),
		}
		e.updateBinlogCoverage()
		assert.Equal(t, float64(0), gaugeValue(e.binlogCoverage))
	})

	t.Run("zero when backup start is zero", func(t *testing.T) {
		e := &Exporter{
			lastLatestBinlogTime: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
			binlogCoverage:       newTestGauge(),
		}
		e.updateBinlogCoverage()
		assert.Equal(t, float64(0), gaugeValue(e.binlogCoverage))
	})

	t.Run("clamps negative coverage to zero", func(t *testing.T) {
		e := &Exporter{
			lastOldestBackupStart: time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC),
			lastLatestBinlogTime:  time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			binlogCoverage:        newTestGauge(),
		}
		e.updateBinlogCoverage()
		assert.Equal(t, float64(0), gaugeValue(e.binlogCoverage))
	})
}
