package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	walgListTimeout         = 45 * time.Second
	walgStorageCheckTimeout = 10 * time.Second
)

// Config is the configuration of the Exporter.
type Config struct {
	// MetricPrefix is the prefix of all metric names, for example walg_mysql.
	MetricPrefix          string
	WalgPath              string
	WalgConfigPath        string
	StorageScrapeInterval time.Duration
	WalgScrapeInterval    time.Duration
}

// Exporter implements the Prometheus Collector interface for MySQL/MariaDB WAL-G.
type Exporter struct {
	logger *slog.Logger
	cfg    Config

	backupCount            *prometheus.GaugeVec
	backupStartTimestamp   *prometheus.GaugeVec
	backupFinishTimestamp  *prometheus.GaugeVec
	backupUncompressedSize *prometheus.GaugeVec
	backupCompressedSize   *prometheus.GaugeVec
	backupDuration         *prometheus.GaugeVec

	binlogCount           prometheus.Gauge
	binlogLatestTimestamp prometheus.Gauge
	binlogOldestTimestamp prometheus.Gauge
	binlogTotalSize       prometheus.Gauge

	rpoSeconds       prometheus.Gauge
	binlogGapSeconds prometheus.Gauge

	storageUp      prometheus.Gauge
	storageLatency prometheus.Gauge

	storageScrapeDuration prometheus.Gauge
	walgScrapeDuration    prometheus.Gauge
	backupScrapeDuration  prometheus.Gauge
	binlogScrapeDuration  prometheus.Gauge

	scrapeErrors prometheus.Counter
	errors       *prometheus.CounterVec

	successfulStorageScrapes atomic.Int64
	successfulWalgScrapes    atomic.Int64
}

// BackupInfo represents a single backup from backup-list --detail --json.
type BackupInfo struct {
	BackupName       string      `json:"backup_name"`
	ModifyTime       time.Time   `json:"modify_time"`
	BinLogStart      string      `json:"binlog_start"`
	BinLogEnd        string      `json:"binlog_end"`
	StartLocalTime   time.Time   `json:"start_local_time"`
	StopLocalTime    time.Time   `json:"stop_local_time"`
	UncompressedSize int64       `json:"uncompressed_size"`
	CompressedSize   int64       `json:"compressed_size"`
	Hostname         string      `json:"hostname"`
	IsPermanent      bool        `json:"is_permanent"`
	UserData         interface{} `json:"user_data,omitempty"`
}

func (b *BackupInfo) duration() float64 {
	if b.StopLocalTime.IsZero() || b.StartLocalTime.IsZero() {
		return 0
	}
	return b.StopLocalTime.Sub(b.StartLocalTime).Seconds()
}

// BinlogInfo represents a single binlog entry from binlog-list --json.
// Field order follows internal/databases/mysql/binlog_list_handler.go.
type BinlogInfo struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size,omitempty"`
	LastModified time.Time `json:"last_modified,omitempty"`
}

func newGauge(namespace, name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help})
}

func newGaugeVec(namespace, name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: help}, labels)
}

// NewExporter creates a new Exporter. The metric names start with cfg.MetricPrefix.
func NewExporter(logger *slog.Logger, cfg Config) *Exporter {
	ns := cfg.MetricPrefix

	e := &Exporter{
		logger: logger,
		cfg:    cfg,

		backupCount:            newGaugeVec(ns, "backups", "Number of backups in storage by type", "backup_type"),
		backupStartTimestamp:   newGaugeVec(ns, "backup_start_timestamp", "Start time of the backup in Unix time", "backup_name"),
		backupFinishTimestamp:  newGaugeVec(ns, "backup_finish_timestamp", "Finish time of the backup in Unix time", "backup_name"),
		backupUncompressedSize: newGaugeVec(ns, "backup_uncompressed_size_bytes", "Uncompressed size of the backup in bytes", "backup_name"),
		backupCompressedSize:   newGaugeVec(ns, "backup_compressed_size_bytes", "Compressed size of the backup in bytes", "backup_name"),
		backupDuration:         newGaugeVec(ns, "backup_duration_seconds", "Duration of the backup in seconds", "backup_name"),

		binlogCount:           newGauge(ns, "binlog_count", "Number of binlogs in storage"),
		binlogLatestTimestamp: newGauge(ns, "binlog_latest_timestamp", "Unix timestamp of the most recently archived binlog"),
		binlogOldestTimestamp: newGauge(ns, "binlog_oldest_timestamp", "Unix timestamp of the oldest binlog in storage"),
		binlogTotalSize:       newGauge(ns, "binlog_total_size_bytes", "Total size of all binlogs in storage in bytes"),

		rpoSeconds: newGauge(ns, "rpo_seconds",
			"Seconds since the most recently archived binlog; the maximum data loss window on failure"),
		binlogGapSeconds: newGauge(ns, "binlog_gap_seconds",
			"Gap between the oldest binlog and the latest backup finish; positive means PITR has a hole"),

		storageUp:      newGauge(ns, "storage_up", "Storage connectivity status (1 = reachable, 0 = unreachable)"),
		storageLatency: newGauge(ns, "storage_latency_seconds", "Storage operation latency in seconds"),

		storageScrapeDuration: newGauge(ns, "storage_scrape_duration_seconds", "Duration of the last storage aliveness check in seconds"),
		walgScrapeDuration: newGauge(ns, "walg_scrape_duration_seconds",
			"Duration of the last WAL-G scrape (backup-list + binlog-list) in seconds"),
		backupScrapeDuration: newGauge(ns, "backup_scrape_duration_seconds", "Duration of the last backup-list command in seconds"),
		binlogScrapeDuration: newGauge(ns, "binlog_scrape_duration_seconds", "Duration of the last binlog-list command in seconds"),

		scrapeErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "scrape_errors_total", Help: "Total number of scrape errors",
		}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "errors_total", Help: "Total number of errors by operation and type",
		}, []string{"operation", "error_type"}),
	}

	return e
}

// IsReady reports whether at least one storage check and one WAL-G scrape have succeeded.
func (e *Exporter) IsReady() bool {
	return e.successfulStorageScrapes.Load() > 0 && e.successfulWalgScrapes.Load() > 0
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.backupCount.Describe(ch)
	e.backupStartTimestamp.Describe(ch)
	e.backupFinishTimestamp.Describe(ch)
	e.backupUncompressedSize.Describe(ch)
	e.backupCompressedSize.Describe(ch)
	e.backupDuration.Describe(ch)
	e.binlogCount.Describe(ch)
	e.binlogLatestTimestamp.Describe(ch)
	e.binlogOldestTimestamp.Describe(ch)
	e.binlogTotalSize.Describe(ch)
	e.rpoSeconds.Describe(ch)
	e.binlogGapSeconds.Describe(ch)
	e.storageUp.Describe(ch)
	e.storageLatency.Describe(ch)
	e.storageScrapeDuration.Describe(ch)
	e.walgScrapeDuration.Describe(ch)
	e.backupScrapeDuration.Describe(ch)
	e.binlogScrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.errors.Describe(ch)
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.backupCount.Collect(ch)
	e.backupStartTimestamp.Collect(ch)
	e.backupFinishTimestamp.Collect(ch)
	e.backupUncompressedSize.Collect(ch)
	e.backupCompressedSize.Collect(ch)
	e.backupDuration.Collect(ch)
	e.binlogCount.Collect(ch)
	e.binlogLatestTimestamp.Collect(ch)
	e.binlogOldestTimestamp.Collect(ch)
	e.binlogTotalSize.Collect(ch)
	e.rpoSeconds.Collect(ch)
	e.binlogGapSeconds.Collect(ch)
	e.storageUp.Collect(ch)
	e.storageLatency.Collect(ch)
	e.storageScrapeDuration.Collect(ch)
	e.walgScrapeDuration.Collect(ch)
	e.backupScrapeDuration.Collect(ch)
	e.binlogScrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.errors.Collect(ch)
}

// Start runs the metric collection loop until ctx is canceled.
func (e *Exporter) Start(ctx context.Context) {
	tickerStorage := time.NewTicker(e.cfg.StorageScrapeInterval)
	tickerWalg := time.NewTicker(e.cfg.WalgScrapeInterval)
	defer tickerStorage.Stop()
	defer tickerWalg.Stop()

	e.checkStorageAliveness(ctx)
	e.scrapeAllWalgMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("Exporter context canceled, stopping metrics collection")
			return
		case <-tickerStorage.C:
			e.checkStorageAliveness(ctx)
		case <-tickerWalg.C:
			e.scrapeAllWalgMetrics(ctx)
		}
	}
}

func (e *Exporter) scrapeAllWalgMetrics(ctx context.Context) {
	start := time.Now()
	scrapeOK := true

	defer func() {
		e.walgScrapeDuration.Set(time.Since(start).Seconds())
		if scrapeOK {
			e.successfulWalgScrapes.Add(1)
		}
		e.logger.Info("WAL-G metrics scrape completed", "duration", time.Since(start), "ok", scrapeOK)
	}()

	now := time.Now()

	backupStart := time.Now()
	backups, err := e.getBackupList(ctx)
	e.backupScrapeDuration.Set(time.Since(backupStart).Seconds())
	if err != nil {
		e.logger.Error("WAL-G command failed", "error", err, "operation", "backup-list")
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		scrapeOK = false
		return
	}

	binlogStart := time.Now()
	binlogs, err := e.getBinlogList(ctx)
	e.binlogScrapeDuration.Set(time.Since(binlogStart).Seconds())
	if err != nil {
		e.logger.Error("WAL-G command failed", "error", err, "operation", "binlog-list")
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("binlog-list", "command_failed").Inc()
		scrapeOK = false
		return
	}

	e.updateBackupMetrics(backups)
	e.updateBinlogMetrics(binlogs, now)
	e.updateBinlogGap(backups, binlogs)
}

// walgOutput runs wal-g with the given arguments, adding --config when it is set, and returns its stdout.
func (e *Exporter) walgOutput(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	if e.cfg.WalgConfigPath != "" {
		args = append(args, "--config", e.cfg.WalgConfigPath)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	output, err := exec.CommandContext(ctx, e.cfg.WalgPath, args...).Output()
	if err != nil {
		return nil, fmt.Errorf("run %s: %w", args[0], err)
	}
	return output, nil
}

func (e *Exporter) getBackupList(ctx context.Context) ([]BackupInfo, error) {
	output, err := e.walgOutput(ctx, walgListTimeout, "backup-list", "--detail", "--json")
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, nil
	}

	var backups []BackupInfo
	if err := json.Unmarshal(output, &backups); err != nil {
		return nil, fmt.Errorf("parse backup-list output: %w", err)
	}
	return backups, nil
}

func (e *Exporter) getBinlogList(ctx context.Context) ([]BinlogInfo, error) {
	output, err := e.walgOutput(ctx, walgListTimeout, "binlog-list", "--json")
	if err != nil {
		return nil, err
	}
	// binlog-list prints nothing when there are no binlogs
	if len(bytes.TrimSpace(output)) == 0 {
		return nil, nil
	}

	var binlogs []BinlogInfo
	if err := json.Unmarshal(output, &binlogs); err != nil {
		return nil, fmt.Errorf("parse binlog-list output: %w", err)
	}
	return binlogs, nil
}

// updateBackupMetrics exports the stats of the stored backups, one series per finished backup. Backups are not
// distinguished by type: wal-g does not report it in backup-list, so all of them are counted as "full".
func (e *Exporter) updateBackupMetrics(backups []BackupInfo) {
	e.backupCount.Reset()
	e.backupStartTimestamp.Reset()
	e.backupFinishTimestamp.Reset()
	e.backupUncompressedSize.Reset()
	e.backupCompressedSize.Reset()
	e.backupDuration.Reset()

	e.backupCount.WithLabelValues("full").Set(float64(len(backups)))

	for i := range backups {
		b := &backups[i]
		if b.StopLocalTime.IsZero() {
			continue
		}
		e.backupStartTimestamp.WithLabelValues(b.BackupName).Set(float64(b.StartLocalTime.Unix()))
		e.backupFinishTimestamp.WithLabelValues(b.BackupName).Set(float64(b.StopLocalTime.Unix()))
		e.backupUncompressedSize.WithLabelValues(b.BackupName).Set(float64(b.UncompressedSize))
		e.backupCompressedSize.WithLabelValues(b.BackupName).Set(float64(b.CompressedSize))
		e.backupDuration.WithLabelValues(b.BackupName).Set(b.duration())
	}

	e.logger.Info("Updated backup metrics", "total", len(backups))
}

func (e *Exporter) updateBinlogMetrics(binlogs []BinlogInfo, now time.Time) {
	if len(binlogs) == 0 {
		e.binlogCount.Set(0)
		e.binlogLatestTimestamp.Set(0)
		e.binlogOldestTimestamp.Set(0)
		e.binlogTotalSize.Set(0)
		e.rpoSeconds.Set(0)
		return
	}

	e.binlogCount.Set(float64(len(binlogs)))

	var latestTime, oldestTime time.Time
	var totalSize int64
	for _, binlog := range binlogs {
		totalSize += binlog.Size
		if binlog.LastModified.IsZero() {
			continue
		}
		if oldestTime.IsZero() || binlog.LastModified.Before(oldestTime) {
			oldestTime = binlog.LastModified
		}
		if binlog.LastModified.After(latestTime) {
			latestTime = binlog.LastModified
		}
	}

	e.binlogTotalSize.Set(float64(totalSize))
	if !latestTime.IsZero() {
		e.binlogLatestTimestamp.Set(float64(latestTime.Unix()))
		e.rpoSeconds.Set(now.Sub(latestTime).Seconds())
	}
	if !oldestTime.IsZero() {
		e.binlogOldestTimestamp.Set(float64(oldestTime.Unix()))
	}

	e.logger.Info("Updated binlog metrics", "count", len(binlogs), "total_size_bytes", totalSize)
}

// updateBinlogGap sets the gap between the oldest binlog and the latest backup finish.
// A positive value means that the binlogs do not reach back to the latest backup, so PITR has a hole.
func (e *Exporter) updateBinlogGap(backups []BackupInfo, binlogs []BinlogInfo) {
	var newestBackupFinish time.Time
	for i := range backups {
		stop := backups[i].StopLocalTime
		if !stop.IsZero() && stop.After(newestBackupFinish) {
			newestBackupFinish = stop
		}
	}

	var oldestBinlog time.Time
	for _, bl := range binlogs {
		if bl.LastModified.IsZero() {
			continue
		}
		if oldestBinlog.IsZero() || bl.LastModified.Before(oldestBinlog) {
			oldestBinlog = bl.LastModified
		}
	}

	if newestBackupFinish.IsZero() || oldestBinlog.IsZero() {
		e.binlogGapSeconds.Set(0)
		return
	}

	gap := oldestBinlog.Sub(newestBackupFinish).Seconds()
	e.binlogGapSeconds.Set(gap)
}

func (e *Exporter) checkStorageAliveness(ctx context.Context) {
	start := time.Now()
	_, err := e.walgOutput(ctx, walgStorageCheckTimeout, "st", "check", "read")
	latency := time.Since(start).Seconds()

	e.storageLatency.Set(latency)
	e.storageScrapeDuration.Set(latency)

	if err != nil {
		e.logger.Error("Storage aliveness check failed", "error", err, "duration", latency)
		e.storageUp.Set(0)
		e.errors.WithLabelValues("storage-check", "connectivity_failed").Inc()
		return
	}

	e.storageUp.Set(1)
	e.successfulStorageScrapes.Add(1)
	e.logger.Info("Storage check completed", "duration", latency)
}
