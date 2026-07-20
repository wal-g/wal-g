package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Exporter implements the Prometheus Collector interface for MySQL/MariaDB WAL-G.
type Exporter struct {
	logger         *slog.Logger
	walgPath       string
	walgConfigPath string

	backupScrapeInterval  time.Duration
	binlogScrapeInterval  time.Duration
	storageScrapeInterval time.Duration

	// last known values used for cross-scrape coverage computation
	lastOldestBackupStart time.Time
	lastLatestBinlogTime  time.Time

	backupCount            *prometheus.GaugeVec
	backupInfo             *prometheus.GaugeVec
	backupStartTimestamp   *prometheus.GaugeVec
	backupFinishTimestamp  *prometheus.GaugeVec
	backupUncompressedSize *prometheus.GaugeVec
	backupCompressedSize   *prometheus.GaugeVec
	backupDuration         *prometheus.GaugeVec
	backupScrapeDuration   prometheus.Gauge

	binlogCount            prometheus.Gauge
	binlogLatestTimestamp  prometheus.Gauge
	binlogTotalSize        prometheus.Gauge
	binlogCoverage         prometheus.Gauge
	binlogScrapeDuration   prometheus.Gauge

	storageAlive         prometheus.Gauge
	storageLatency       prometheus.Gauge

	scrapeErrors prometheus.Counter
	errors       *prometheus.CounterVec
}

// BackupInfo represents an entry from wal-g backup-list --detail --json.
type BackupInfo struct {
	BackupName       string    `json:"backup_name"`
	StartLocalTime   time.Time `json:"start_local_time"`
	StopLocalTime    time.Time `json:"stop_local_time"`
	BinLogStart      string    `json:"binlog_start"`
	BinLogEnd        string    `json:"binlog_end"`
	UncompressedSize int64     `json:"uncompressed_size"`
	CompressedSize   int64     `json:"compressed_size"`
	Hostname         string    `json:"hostname"`
	IsPermanent      bool      `json:"is_permanent"`
}

func (b *BackupInfo) backupType() string {
	if strings.Contains(b.BackupName, "_increment") || strings.Contains(b.BackupName, "_incr") {
		return "incremental"
	}
	return "full"
}

func (b *BackupInfo) duration() float64 {
	if b.StopLocalTime.IsZero() || b.StartLocalTime.IsZero() {
		return 0
	}
	return b.StopLocalTime.Sub(b.StartLocalTime).Seconds()
}

// BinlogInfo represents an entry from wal-g binlog-list.
type BinlogInfo struct {
	Name         string
	ModifiedTime time.Time
	Size         int64
}

// NewExporter creates a new Exporter. The caller owns the logger.
func NewExporter(
	logger *slog.Logger,
	walgPath string,
	backupScrapeInterval time.Duration,
	binlogScrapeInterval time.Duration,
	storageScrapeInterval time.Duration,
	walgConfigPath string,
) (*Exporter, error) {
	return &Exporter{
		logger:                logger,
		walgPath:              walgPath,
		walgConfigPath:        walgConfigPath,
		backupScrapeInterval:  backupScrapeInterval,
		binlogScrapeInterval:  binlogScrapeInterval,
		storageScrapeInterval: storageScrapeInterval,

		backupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backups_total",
				Help: "Number of backups by type.",
			},
			[]string{"backup_type"},
		),

		backupInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_info",
				Help: "Metadata for each stored backup. Value is always 1.",
			},
			[]string{"backup_name", "backup_type", "hostname", "is_permanent", "binlog_start", "binlog_end"},
		),

		backupStartTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_start_timestamp",
				Help: "Start time of the backup (Unix timestamp).",
			},
			[]string{"backup_name"},
		),

		backupFinishTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_finish_timestamp",
				Help: "Finish time of the backup (Unix timestamp).",
			},
			[]string{"backup_name"},
		),

		backupUncompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_uncompressed_size_bytes",
				Help: "Uncompressed size of the backup in bytes.",
			},
			[]string{"backup_name"},
		),

		backupCompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_compressed_size_bytes",
				Help: "Compressed size of the backup in bytes.",
			},
			[]string{"backup_name"},
		),

		backupDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_duration_seconds",
				Help: "Duration of the backup operation in seconds.",
			},
			[]string{"backup_name"},
		),

		backupScrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_list_duration_seconds",
				Help: "Time taken to execute backup-list during the last collector run.",
			},
		),

		binlogCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_count",
				Help: "Number of binlogs in storage.",
			},
		),

		binlogLatestTimestamp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_latest_timestamp",
				Help: "Modification time of the most recent binlog (Unix timestamp).",
			},
		),

		binlogTotalSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_total_size_bytes",
				Help: "Total size of all binlogs in storage in bytes.",
			},
		),

		binlogCoverage: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_coverage_seconds",
				Help: "Time span covered by stored binlogs in seconds (latest binlog time minus oldest backup start time).",
			},
		),

		binlogScrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_list_duration_seconds",
				Help: "Time taken to execute binlog-list during the last collector run.",
			},
		),

		storageAlive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_storage_up",
				Help: "Storage connectivity status (1 = up, 0 = down).",
			},
		),

		storageLatency: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_storage_latency_seconds",
				Help: "Storage operation latency in seconds.",
			},
		),

		scrapeErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "walg_mysql_scrape_errors_total",
				Help: "Total number of scrape errors.",
			},
		),

		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walg_mysql_errors_total",
				Help: "Total number of WAL-G errors by operation and error type.",
			},
			[]string{"operation", "error_type"},
		),
	}, nil
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.backupCount.Describe(ch)
	e.backupInfo.Describe(ch)
	e.backupStartTimestamp.Describe(ch)
	e.backupFinishTimestamp.Describe(ch)
	e.backupUncompressedSize.Describe(ch)
	e.backupCompressedSize.Describe(ch)
	e.backupDuration.Describe(ch)
	e.backupScrapeDuration.Describe(ch)
	e.binlogCount.Describe(ch)
	e.binlogLatestTimestamp.Describe(ch)
	e.binlogTotalSize.Describe(ch)
	e.binlogCoverage.Describe(ch)
	e.binlogScrapeDuration.Describe(ch)
	e.storageAlive.Describe(ch)
	e.storageLatency.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.errors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.backupCount.Collect(ch)
	e.backupInfo.Collect(ch)
	e.backupStartTimestamp.Collect(ch)
	e.backupFinishTimestamp.Collect(ch)
	e.backupUncompressedSize.Collect(ch)
	e.backupCompressedSize.Collect(ch)
	e.backupDuration.Collect(ch)
	e.backupScrapeDuration.Collect(ch)
	e.binlogCount.Collect(ch)
	e.binlogLatestTimestamp.Collect(ch)
	e.binlogTotalSize.Collect(ch)
	e.binlogCoverage.Collect(ch)
	e.binlogScrapeDuration.Collect(ch)
	e.storageAlive.Collect(ch)
	e.storageLatency.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.errors.Collect(ch)
}

// Start runs the metric collection loop until ctx is cancelled.
func (e *Exporter) Start(ctx context.Context) {
	tickerBackup := time.NewTicker(e.backupScrapeInterval)
	defer tickerBackup.Stop()
	tickerBinlog := time.NewTicker(e.binlogScrapeInterval)
	defer tickerBinlog.Stop()
	tickerStorage := time.NewTicker(e.storageScrapeInterval)
	defer tickerStorage.Stop()

	e.checkStorage()
	e.scrapeBackups()
	e.scrapeBinlogs()

	e.logger.Info("Initial scrape completed; starting periodic collection")

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Context cancelled, stopping collection")
			return
		case <-tickerBackup.C:
			e.scrapeBackups()
		case <-tickerBinlog.C:
			e.scrapeBinlogs()
		case <-tickerStorage.C:
			e.checkStorage()
		}
	}
}

func (e *Exporter) scrapeBackups() {
	start := time.Now()
	defer func() { e.backupScrapeDuration.Set(time.Since(start).Seconds()) }()

	backups, err := e.fetchBackupList()
	if err != nil {
		e.logger.Error("backup-list failed", "error", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		return
	}

	e.updateBackupMetrics(backups)
	e.lastOldestBackupStart = oldestNonPermanentStart(backups)
	e.updateBinlogCoverage()
	e.logger.Info("backup-list scrape completed", "count", len(backups), "duration", time.Since(start))
}

func (e *Exporter) scrapeBinlogs() {
	start := time.Now()
	defer func() { e.binlogScrapeDuration.Set(time.Since(start).Seconds()) }()

	binlogs, err := e.fetchBinlogList()
	if err != nil {
		e.logger.Error("binlog-list failed", "error", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("binlog-list", "command_failed").Inc()
		return
	}

	e.lastLatestBinlogTime = latestBinlogTime(binlogs)
	e.updateBinlogMetrics(binlogs)
	e.updateBinlogCoverage()
	e.logger.Info("binlog-list scrape completed", "count", len(binlogs), "duration", time.Since(start))
}

func (e *Exporter) fetchBackupList() ([]BackupInfo, error) {
	args := []string{"backup-list", "--detail", "--json"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}

	output, err := exec.Command(e.walgPath, args...).Output()
	if err != nil {
		return nil, fmt.Errorf("backup-list: %w", err)
	}

	var backups []BackupInfo
	if err := json.Unmarshal(output, &backups); err != nil {
		return nil, fmt.Errorf("parse backup-list output: %w", err)
	}

	return backups, nil
}

func (e *Exporter) fetchBinlogList() ([]BinlogInfo, error) {
	args := []string{"binlog-list"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}

	output, err := exec.Command(e.walgPath, args...).Output()
	if err != nil {
		// binlog-list fails when no binlogs exist; treat as empty, not an error.
		return nil, nil
	}

	return parseBinlogList(string(output))
}

// parseBinlogList parses the text output of wal-g binlog-list.
// Expected format per line: <name> <RFC3339-timestamp> <size-bytes>
// The first line is skipped if it cannot be parsed as data (header row detection).
func parseBinlogList(output string) ([]BinlogInfo, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	binlogs := make([]BinlogInfo, 0, len(lines))

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, fmt.Errorf("line %d: expected at least 3 fields, got %d: %q", i+1, len(fields), line)
		}

		modTime, err := time.Parse(time.RFC3339, fields[1])
		if err != nil {
			if i == 0 {
				// First line with non-parseable timestamp is treated as a header.
				continue
			}
			return nil, fmt.Errorf("line %d: parse timestamp %q: %w", i+1, fields[1], err)
		}

		size, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: parse size %q: %w", i+1, fields[2], err)
		}

		binlogs = append(binlogs, BinlogInfo{
			Name:         fields[0],
			ModifiedTime: modTime,
			Size:         size,
		})
	}

	return binlogs, nil
}

func (e *Exporter) updateBackupMetrics(backups []BackupInfo) {
	e.backupCount.Reset()
	e.backupInfo.Reset()
	e.backupStartTimestamp.Reset()
	e.backupFinishTimestamp.Reset()
	e.backupUncompressedSize.Reset()
	e.backupCompressedSize.Reset()
	e.backupDuration.Reset()

	counts := map[string]float64{"full": 0, "incremental": 0}

	for _, b := range backups {
		bt := b.backupType()
		counts[bt]++

		e.backupInfo.WithLabelValues(
			b.BackupName,
			bt,
			b.Hostname,
			strconv.FormatBool(b.IsPermanent),
			b.BinLogStart,
			b.BinLogEnd,
		).Set(1)

		e.backupStartTimestamp.WithLabelValues(b.BackupName).Set(float64(b.StartLocalTime.Unix()))
		e.backupFinishTimestamp.WithLabelValues(b.BackupName).Set(float64(b.StopLocalTime.Unix()))
		e.backupUncompressedSize.WithLabelValues(b.BackupName).Set(float64(b.UncompressedSize))
		e.backupCompressedSize.WithLabelValues(b.BackupName).Set(float64(b.CompressedSize))
		e.backupDuration.WithLabelValues(b.BackupName).Set(b.duration())
	}

	for bt, count := range counts {
		e.backupCount.WithLabelValues(bt).Set(count)
	}
}

func (e *Exporter) updateBinlogMetrics(binlogs []BinlogInfo) {
	e.binlogCount.Set(float64(len(binlogs)))

	if len(binlogs) == 0 {
		e.binlogLatestTimestamp.Set(0)
		e.binlogTotalSize.Set(0)
		return
	}

	var totalSize int64
	for _, b := range binlogs {
		totalSize += b.Size
	}

	e.binlogTotalSize.Set(float64(totalSize))
	e.binlogLatestTimestamp.Set(float64(e.lastLatestBinlogTime.Unix()))
}

func (e *Exporter) updateBinlogCoverage() {
	if e.lastLatestBinlogTime.IsZero() || e.lastOldestBackupStart.IsZero() {
		e.binlogCoverage.Set(0)
		return
	}

	coverage := e.lastLatestBinlogTime.Sub(e.lastOldestBackupStart).Seconds()
	if coverage < 0 {
		coverage = 0
	}
	e.binlogCoverage.Set(coverage)
}

func oldestNonPermanentStart(backups []BackupInfo) time.Time {
	var t time.Time
	for _, b := range backups {
		if b.IsPermanent {
			continue
		}
		if t.IsZero() || b.StartLocalTime.Before(t) {
			t = b.StartLocalTime
		}
	}
	return t
}

func latestBinlogTime(binlogs []BinlogInfo) time.Time {
	var t time.Time
	for _, b := range binlogs {
		if b.ModifiedTime.After(t) {
			t = b.ModifiedTime
		}
	}
	return t
}

func (e *Exporter) checkStorage() {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	args := []string{"st", "check", "read"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}

	err := exec.CommandContext(ctx, e.walgPath, args...).Run()
	latency := time.Since(start).Seconds()

	e.storageLatency.Set(latency)

	if err != nil {
		e.logger.Error("Storage check failed", "error", err, "latency", latency)
		e.storageAlive.Set(0)
		e.errors.WithLabelValues("storage-check", "connectivity_failed").Inc()
		return
	}

	e.storageAlive.Set(1)
	e.logger.Info("Storage check completed", "latency", latency)
}
