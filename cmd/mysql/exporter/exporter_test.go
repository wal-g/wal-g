package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBinlogList(t *testing.T) {
	t.Run("valid output", func(t *testing.T) {
		output := `binlog.000001 2026-05-01T10:00:00Z 1024
binlog.000002 2026-05-02T11:00:00Z 2048
binlog.000003 2026-05-03T12:00:00Z 4096`

		binlogs, err := parseBinlogList(output)
		require.NoError(t, err)
		require.Len(t, binlogs, 3)

		assert.Equal(t, "binlog.000001", binlogs[0].Name)
		assert.Equal(t, int64(1024), binlogs[0].Size)
		assert.Equal(t, "binlog.000003", binlogs[2].Name)
		assert.Equal(t, int64(4096), binlogs[2].Size)
	})

	t.Run("empty output", func(t *testing.T) {
		binlogs, err := parseBinlogList("")
		require.NoError(t, err)
		assert.Empty(t, binlogs)
	})

	t.Run("skips header line", func(t *testing.T) {
		output := `name modified size
binlog.000001 2026-05-01T10:00:00Z 512`

		binlogs, err := parseBinlogList(output)
		require.NoError(t, err)
		require.Len(t, binlogs, 1)
		assert.Equal(t, "binlog.000001", binlogs[0].Name)
	})

	t.Run("invalid timestamp returns error", func(t *testing.T) {
		output := `binlog.000001 2026-05-01T10:00:00Z 1024
binlog.000002 not-a-timestamp 2048`

		_, err := parseBinlogList(output)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse timestamp")
	})

	t.Run("invalid size returns error", func(t *testing.T) {
		output := `binlog.000001 2026-05-01T10:00:00Z not-a-size`

		_, err := parseBinlogList(output)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parse size")
	})

	t.Run("insufficient fields returns error", func(t *testing.T) {
		output := `binlog.000001 2026-05-01T10:00:00Z 1024
binlog.000002`

		_, err := parseBinlogList(output)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected at least 3 fields")
	})
}

func TestBackupType(t *testing.T) {
	tests := []struct {
		name       string
		backupName string
		expected   string
	}{
		{"full backup", "stream_20260501T100000Z", "full"},
		{"incremental with _increment", "stream_20260501T100000Z_increment_20260502T100000Z", "incremental"},
		{"incremental with _incr", "stream_20260501T100000Z_incr", "incremental"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &BackupInfo{BackupName: tt.backupName}
			assert.Equal(t, tt.expected, b.backupType())
		})
	}
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
			binlogCoverage: newTestGauge(),
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
