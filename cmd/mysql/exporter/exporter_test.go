package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realBinlogListJSON and realBackupListJSON are outputs of 'wal-g binlog-list --json' and
// 'wal-g backup-list --detail --json' of the MySQL binary.
const realBinlogListJSON = `[
  {"name":"mysql-bin.000001","size":1000,"last_modified":"2026-10-27T17:50:23.625471526+01:00"},
  {"name":"mysql-bin.000002","size":2000,"last_modified":"2026-10-27T17:50:23.631783551+01:00"}
]`

const realBackupListJSON = `[
  {"backup_name":"stream_20260101T000000Z","modify_time":"2026-10-27T17:50:53.340358836+01:00",
   "binlog_start":"mysql-bin.000001","binlog_end":"mysql-bin.000002",
   "start_local_time":"2026-01-01T00:00:00Z","stop_local_time":"2026-01-01T00:05:00Z",
   "uncompressed_size":5000,"compressed_size":1000,"hostname":"db1","is_permanent":false}
]`

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return string(data)
}

// fakeWalg is a stand-in for the wal-g binary that prints the given outputs.
type fakeWalg struct {
	backupList string
	binlogList string
	// failing holds the commands that exit with an error
	failing map[string]bool
}

func (f fakeWalg) install(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "backups.json"), []byte(f.backupList), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "binlogs.json"), []byte(f.binlogList), 0o600))

	var script strings.Builder
	script.WriteString("#!/bin/sh\ncase \"$1\" in\n")
	for cmd, file := range map[string]string{"backup-list": "backups.json", "binlog-list": "binlogs.json"} {
		if f.failing[cmd] {
			script.WriteString("  " + cmd + ") echo failing >&2; exit 1 ;;\n")
		} else {
			script.WriteString("  " + cmd + ") cat " + filepath.Join(dir, file) + " ;;\n")
		}
	}
	if f.failing["st"] {
		script.WriteString("  st) echo failing >&2; exit 1 ;;\n")
	} else {
		script.WriteString("  st) exit 0 ;;\n")
	}
	script.WriteString("  *) echo \"unexpected arguments: $*\" >&2; exit 1 ;;\nesac\n")

	path := filepath.Join(dir, "wal-g")
	require.NoError(t, os.WriteFile(path, []byte(script.String()), 0o700))
	return path
}

func newTestExporter(t *testing.T, prefix string, f fakeWalg) *Exporter {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewExporter(logger, Config{
		MetricPrefix:          prefix,
		WalgPath:              f.install(t),
		StorageScrapeInterval: time.Hour,
		WalgScrapeInterval:    time.Hour,
	})
}

func defaultFake(t *testing.T) fakeWalg {
	t.Helper()
	return fakeWalg{backupList: readTestdata(t, "backup-list.json"), binlogList: readTestdata(t, "binlog-list.json")}
}

func TestMetricNamesUseDatabasePrefix(t *testing.T) {
	for _, namespace := range []string{"walg_mysql", "walg_mariadb"} {
		t.Run(namespace, func(t *testing.T) {
			e := newTestExporter(t, namespace, defaultFake(t))
			e.checkStorageAliveness(context.Background())
			e.scrapeAllWalgMetrics(context.Background())

			registry := prometheus.NewPedanticRegistry()
			registry.MustRegister(e)
			families, err := registry.Gather()
			require.NoError(t, err)

			got := map[string]bool{}
			for _, family := range families {
				got[family.GetName()] = true
				assert.True(t, strings.HasPrefix(family.GetName(), namespace+"_"), "unexpected name %s", family.GetName())
			}

			// errors_total has no series until an error happens
			for _, suffix := range []string{
				"backups", "backup_start_timestamp", "backup_finish_timestamp", "backup_uncompressed_size_bytes",
				"backup_compressed_size_bytes", "backup_duration_seconds", "binlog_count", "binlog_latest_timestamp",
				"binlog_oldest_timestamp", "binlog_total_size_bytes", "rpo_seconds", "binlog_gap_seconds",
				"storage_up", "storage_latency_seconds", "storage_scrape_duration_seconds", "walg_scrape_duration_seconds",
				"backup_scrape_duration_seconds", "binlog_scrape_duration_seconds", "scrape_errors_total",
			} {
				assert.True(t, got[namespace+"_"+suffix], "metric %s_%s is missing", namespace, suffix)
			}
		})
	}
}

func TestMetricPrefix(t *testing.T) {
	for database, want := range map[string]string{"mysql": "walg_mysql", "mariadb": "walg_mariadb"} {
		got, err := metricPrefix(database)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	for _, invalid := range []string{"", "postgres", "MySQL", "mariadb ", "walg_mariadb"} {
		_, err := metricPrefix(invalid)
		assert.Error(t, err, "%q", invalid)
	}
}

func TestBackupAndBinlogMetrics(t *testing.T) {
	e := newTestExporter(t, "walg_mysql", defaultFake(t))
	ctx := context.Background()

	backups, err := e.getBackupList(ctx)
	require.NoError(t, err)
	binlogs, err := e.getBinlogList(ctx)
	require.NoError(t, err)
	require.Len(t, backups, 3)
	require.Len(t, binlogs, 5)

	// one hour after the latest binlog
	now := time.Date(2025, 2, 22, 7, 45, 0, 0, time.UTC)
	e.updateBackupMetrics(backups)
	e.updateBinlogMetrics(binlogs, now)
	e.updateBinlogGap(backups, binlogs)

	assert.Equal(t, 3.0, testutil.ToFloat64(e.backupCount.WithLabelValues("full")))
	assert.Equal(t, 99.0, testutil.ToFloat64(e.backupDuration.WithLabelValues("stream_20250222T020001Z")))
	assert.Equal(t, 927914660.0, testutil.ToFloat64(e.backupCompressedSize.WithLabelValues("stream_20250222T020001Z")))
	assert.Equal(t, 2782971598.0, testutil.ToFloat64(e.backupUncompressedSize.WithLabelValues("stream_20250222T020001Z")))
	assert.Equal(t, float64(time.Date(2025, 2, 22, 2, 1, 40, 0, time.UTC).Unix()), testutil.ToFloat64(e.backupFinishTimestamp.WithLabelValues("stream_20250222T020001Z")))
	// one series per finished backup
	assert.Equal(t, 3, testutil.CollectAndCount(e.backupFinishTimestamp))

	assert.Equal(t, 5.0, testutil.ToFloat64(e.binlogCount))
	assert.Equal(t, 6029312.0, testutil.ToFloat64(e.binlogTotalSize))
	assert.Equal(t, float64(time.Date(2025, 2, 22, 6, 45, 0, 0, time.UTC).Unix()), testutil.ToFloat64(e.binlogLatestTimestamp))
	assert.Equal(t, float64(time.Date(2025, 2, 21, 1, 30, 0, 0, time.UTC).Unix()), testutil.ToFloat64(e.binlogOldestTimestamp))
	assert.Equal(t, 3600.0, testutil.ToFloat64(e.rpoSeconds))

	// the oldest binlog (Feb 21 01:30:00) is older than the latest backup finish (Feb 22 02:01:40): no hole, the gap is negative
	assert.Equal(t, -(24*3600 + 31*60 + 40.0), testutil.ToFloat64(e.binlogGapSeconds))
}

func TestBinlogGapIsPositiveWhenBinlogsDoNotReachTheBackup(t *testing.T) {
	e := newTestExporter(t, "walg_mysql", fakeWalg{backupList: realBackupListJSON, binlogList: "[]"})

	backups := []BackupInfo{{BackupName: "b", StopLocalTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}}
	binlogs := []BinlogInfo{{Name: "bin.1", LastModified: time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)}}
	e.updateBinlogGap(backups, binlogs)
	assert.Equal(t, 3600.0, testutil.ToFloat64(e.binlogGapSeconds))

	e.updateBinlogGap(nil, binlogs)
	assert.Equal(t, 0.0, testutil.ToFloat64(e.binlogGapSeconds))
}

func TestRealWalgOutputFormat(t *testing.T) {
	e := newTestExporter(t, "walg_mysql", fakeWalg{backupList: realBackupListJSON, binlogList: realBinlogListJSON})
	e.scrapeAllWalgMetrics(context.Background())

	assert.Equal(t, 1.0, testutil.ToFloat64(e.backupCount.WithLabelValues("full")))
	assert.Equal(t, 5000.0, testutil.ToFloat64(e.backupUncompressedSize.WithLabelValues("stream_20260101T000000Z")))
	assert.Equal(t, 2.0, testutil.ToFloat64(e.binlogCount))
	assert.Equal(t, 3000.0, testutil.ToFloat64(e.binlogTotalSize))
	assert.Equal(t, 0.0, testutil.ToFloat64(e.scrapeErrors))
}

func TestNoBackupsAndNoBinlogs(t *testing.T) {
	// wal-g prints an empty list for backups and nothing for binlogs
	e := newTestExporter(t, "walg_mysql", fakeWalg{backupList: "[]", binlogList: ""})
	e.scrapeAllWalgMetrics(context.Background())

	assert.Equal(t, 0.0, testutil.ToFloat64(e.backupCount.WithLabelValues("full")))
	assert.Equal(t, 0.0, testutil.ToFloat64(e.binlogCount))
	assert.Equal(t, 0.0, testutil.ToFloat64(e.scrapeErrors))
	assert.True(t, e.successfulWalgScrapes.Load() > 0)
}

func TestFailingCommandsAreReported(t *testing.T) {
	for _, command := range []string{"backup-list", "binlog-list"} {
		t.Run(command, func(t *testing.T) {
			f := defaultFake(t)
			f.failing = map[string]bool{command: true}
			e := newTestExporter(t, "walg_mysql", f)
			e.scrapeAllWalgMetrics(context.Background())

			assert.Equal(t, 1.0, testutil.ToFloat64(e.scrapeErrors))
			assert.Equal(t, 1.0, testutil.ToFloat64(e.errors.WithLabelValues(command, "command_failed")))
			assert.Equal(t, int64(0), e.successfulWalgScrapes.Load())
		})
	}
}

func TestStorageCheck(t *testing.T) {
	e := newTestExporter(t, "walg_mysql", defaultFake(t))
	e.checkStorageAliveness(context.Background())
	assert.Equal(t, 1.0, testutil.ToFloat64(e.storageUp))

	f := defaultFake(t)
	f.failing = map[string]bool{"st": true}
	e = newTestExporter(t, "walg_mysql", f)
	e.checkStorageAliveness(context.Background())
	assert.Equal(t, 0.0, testutil.ToFloat64(e.storageUp))
	assert.Equal(t, 1.0, testutil.ToFloat64(e.errors.WithLabelValues("storage-check", "connectivity_failed")))
}

func TestHTTPEndpoints(t *testing.T) {
	e := newTestExporter(t, "walg_mariadb", defaultFake(t))
	registry := prometheus.NewRegistry()
	registry.MustRegister(e)
	server := httptest.NewServer(newHandler(e, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), "/metrics"))
	t.Cleanup(server.Close)

	get := func(path string) (int, string) {
		resp, err := http.Get(server.URL + path)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, string(body)
	}

	status, _ := get("/health")
	assert.Equal(t, http.StatusOK, status)

	status, _ = get("/ready")
	assert.Equal(t, http.StatusServiceUnavailable, status, "not ready before the first scrape")

	e.checkStorageAliveness(context.Background())
	e.scrapeAllWalgMetrics(context.Background())

	status, _ = get("/ready")
	assert.Equal(t, http.StatusOK, status)

	status, body := get("/metrics")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "walg_mariadb_backups{backup_type=\"full\"} 3")
	assert.Contains(t, body, "walg_mariadb_binlog_count 5")
	assert.Contains(t, body, "walg_mariadb_storage_up 1")
}
