package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MySQLWalgExporter implements the Prometheus Collector interface for MySQL/MariaDB
type MySQLWalgExporter struct {
	walgPath       string
	walgConfigPath string
	scrapeInterval time.Duration

	// Metrics
	backupCount            *prometheus.GaugeVec
	backupStartTimestamp   *prometheus.GaugeVec
	backupFinishTimestamp  *prometheus.GaugeVec
	backupUncompressedSize *prometheus.GaugeVec
	backupCompressedSize   *prometheus.GaugeVec
	backupDuration         *prometheus.GaugeVec
	binlogCount            prometheus.Gauge
	binlogLatestTimestamp  prometheus.Gauge
	binlogTotalSize        prometheus.Gauge
	scrapeDuration         prometheus.Gauge
	scrapeErrors           prometheus.Counter
	errors                 *prometheus.CounterVec

	// Storage health metrics
	storageAlive   prometheus.Gauge
	storageLatency prometheus.Gauge

	// Internal state
	lastScrape time.Time
}

// MySQLBackupInfo represents backup information from backup-list --detail --json
type MySQLBackupInfo struct {
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

// GetBackupType determines if backup is full or incremental
func (b *MySQLBackupInfo) GetBackupType() string {
	// Check if backup name contains increment indicators
	// Incremental backups created by xtrabackup have specific naming patterns
	if strings.Contains(b.BackupName, "_increment") || strings.Contains(b.BackupName, "_incr") {
		return "incremental"
	}
	return "full"
}

// GetBackupDuration calculates backup duration in seconds
func (b *MySQLBackupInfo) GetBackupDuration() float64 {
	if b.StopLocalTime.IsZero() || b.StartLocalTime.IsZero() {
		return 0
	}
	return b.StopLocalTime.Sub(b.StartLocalTime).Seconds()
}

// MySQLBinlogInfo represents binlog information
type MySQLBinlogInfo struct {
	BinlogName   string    `json:"binlog_name"`
	ModifiedTime time.Time `json:"modified_time"`
	Size         int64     `json:"size"`
}

// NewMySQLWalgExporter creates a new WAL-G exporter for MySQL/MariaDB
func NewMySQLWalgExporter(walgPath string, scrapeInterval time.Duration, walgConfigPath string) (*MySQLWalgExporter, error) {
	return &MySQLWalgExporter{
		walgPath:       walgPath,
		scrapeInterval: scrapeInterval,
		walgConfigPath: walgConfigPath,

		backupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backups",
				Help: "Number of backups by type (full/incremental)",
			},
			[]string{"backup_type"},
		),

		backupStartTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_start_timestamp",
				Help: "Start timestamp of backup in Unix time",
			},
			[]string{"backup_name", "backup_type", "hostname", "is_permanent", "binlog_start", "binlog_end"},
		),

		backupFinishTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_finish_timestamp",
				Help: "Finish timestamp of backup in Unix time",
			},
			[]string{"backup_name", "backup_type", "hostname", "is_permanent", "binlog_start", "binlog_end"},
		),

		backupUncompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_uncompressed_size_bytes",
				Help: "Uncompressed size of the backup in bytes",
			},
			[]string{"backup_name", "backup_type", "hostname"},
		),

		backupCompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_compressed_size_bytes",
				Help: "Compressed size of the backup in bytes",
			},
			[]string{"backup_name", "backup_type", "hostname"},
		),

		backupDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_mysql_backup_duration_seconds",
				Help: "Duration of backup operation in seconds",
			},
			[]string{"backup_name", "backup_type"},
		),

		binlogCount: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_count",
				Help: "Number of binlogs in storage",
			},
		),

		binlogLatestTimestamp: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_latest_timestamp",
				Help: "Timestamp of the latest binlog in Unix time",
			},
		),

		binlogTotalSize: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_binlog_total_size_bytes",
				Help: "Total size of all binlogs in storage in bytes",
			},
		),

		scrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_scrape_duration_seconds",
				Help: "Duration of the last scrape in seconds",
			},
		),

		scrapeErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "walg_mysql_scrape_errors_total",
				Help: "Total number of scrape errors",
			},
		),

		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walg_mysql_errors_total",
				Help: "Total number of WAL-G errors by operation and error type",
			},
			[]string{"operation", "error_type"},
		),

		storageAlive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_storage_alive",
				Help: "Storage connectivity status (1 = alive, 0 = dead)",
			},
		),

		storageLatency: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_mysql_storage_latency_seconds",
				Help: "Storage operation latency in seconds",
			},
		),
	}, nil
}

// Describe implements the Prometheus Collector interface
func (e *MySQLWalgExporter) Describe(ch chan<- *prometheus.Desc) {
	e.backupCount.Describe(ch)
	e.backupStartTimestamp.Describe(ch)
	e.backupFinishTimestamp.Describe(ch)
	e.backupUncompressedSize.Describe(ch)
	e.backupCompressedSize.Describe(ch)
	e.backupDuration.Describe(ch)
	e.binlogCount.Describe(ch)
	e.binlogLatestTimestamp.Describe(ch)
	e.binlogTotalSize.Describe(ch)
	e.scrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.errors.Describe(ch)
	e.storageAlive.Describe(ch)
	e.storageLatency.Describe(ch)
}

// Collect implements the Prometheus Collector interface
func (e *MySQLWalgExporter) Collect(ch chan<- prometheus.Metric) {
	e.backupCount.Collect(ch)
	e.backupStartTimestamp.Collect(ch)
	e.backupFinishTimestamp.Collect(ch)
	e.backupUncompressedSize.Collect(ch)
	e.backupCompressedSize.Collect(ch)
	e.backupDuration.Collect(ch)
	e.binlogCount.Collect(ch)
	e.binlogLatestTimestamp.Collect(ch)
	e.binlogTotalSize.Collect(ch)
	e.scrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.errors.Collect(ch)
	e.storageAlive.Collect(ch)
	e.storageLatency.Collect(ch)
}

// Start begins the metrics collection loop
func (e *MySQLWalgExporter) Start(ctx context.Context) {
	ticker := time.NewTicker(e.scrapeInterval)
	defer ticker.Stop()

	// Initial scrape
	e.scrapeMetrics()

	for {
		select {
		case <-ctx.Done():
			log.Println("Exporter context cancelled, stopping metrics collection")
			return
		case <-ticker.C:
			e.scrapeMetrics()
		}
	}
}

// scrapeMetrics collects all metrics from WAL-G
func (e *MySQLWalgExporter) scrapeMetrics() {
	start := time.Now()
	defer func() {
		e.scrapeDuration.Set(time.Since(start).Seconds())
		e.lastScrape = time.Now()
	}()

	log.Printf("Scraping WAL-G MySQL metrics...")

	// Check storage aliveness first
	e.checkStorageAliveness()

	// Get backup information
	backups, err := e.getBackupList()
	if err != nil {
		log.Printf("Error getting backup list: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		// Continue with other metrics even if backup-list fails
	} else {
		e.updateBackupMetrics(backups)
	}

	// Get binlog information
	binlogs, err := e.getBinlogList()
	if err != nil {
		log.Printf("Error getting binlog list: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("binlog-list", "command_failed").Inc()
		// Continue even if binlog-list fails
	} else {
		e.updateBinlogMetrics(binlogs)
	}

	log.Printf("Metrics scrape completed in %v", time.Since(start))
}

// getBackupList executes wal-g backup-list --detail --json
func (e *MySQLWalgExporter) getBackupList() ([]MySQLBackupInfo, error) {
	var cmd *exec.Cmd

	if e.walgConfigPath != "" {
		cmd = exec.Command(e.walgPath, "backup-list", "--detail", "--json", "--config", e.walgConfigPath)
	} else {
		cmd = exec.Command(e.walgPath, "backup-list", "--detail", "--json")
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute backup-list: %w", err)
	}

	var backups []MySQLBackupInfo
	if err := json.Unmarshal(output, &backups); err != nil {
		return nil, fmt.Errorf("failed to parse backup-list output: %w", err)
	}

	return backups, nil
}

// getBinlogList executes wal-g binlog-list (if available)
func (e *MySQLWalgExporter) getBinlogList() ([]MySQLBinlogInfo, error) {
	var cmd *exec.Cmd

	if e.walgConfigPath != "" {
		cmd = exec.Command(e.walgPath, "binlog-list", "--config", e.walgConfigPath)
	} else {
		cmd = exec.Command(e.walgPath, "binlog-list")
	}

	output, err := cmd.Output()
	if err != nil {
		// binlog-list might not be available or no binlogs exist
		// This is not necessarily an error
		return []MySQLBinlogInfo{}, nil
	}

	// Parse binlog-list output (format: name, modified_time, size)
	// Since binlog-list doesn't output JSON, we parse the text output
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var binlogs []MySQLBinlogInfo

	for i, line := range lines {
		if i == 0 || strings.TrimSpace(line) == "" {
			// Skip header or empty lines
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 3 {
			binlog := MySQLBinlogInfo{
				BinlogName: fields[0],
			}

			// Try to parse timestamp (fields[1])
			if t, err := time.Parse("2006-01-02T15:04:05Z", fields[1]); err == nil {
				binlog.ModifiedTime = t
			}

			// Try to parse size (fields[2])
			if size, err := strconv.ParseInt(fields[2], 10, 64); err == nil {
				binlog.Size = size
			}

			binlogs = append(binlogs, binlog)
		}
	}

	return binlogs, nil
}

// updateBackupMetrics updates backup-related metrics
func (e *MySQLWalgExporter) updateBackupMetrics(backups []MySQLBackupInfo) {
	// Reset metrics
	e.backupCount.Reset()
	e.backupStartTimestamp.Reset()
	e.backupFinishTimestamp.Reset()
	e.backupUncompressedSize.Reset()
	e.backupCompressedSize.Reset()
	e.backupDuration.Reset()

	fullCount, incrementalCount := 0, 0

	for _, backup := range backups {
		backupType := backup.GetBackupType()
		if backupType == "full" {
			fullCount++
		} else {
			incrementalCount++
		}

		permanent := strconv.FormatBool(backup.IsPermanent)

		// Set timestamp metrics with detailed labels
		labels := []string{
			backup.BackupName,
			backupType,
			backup.Hostname,
			permanent,
			backup.BinLogStart,
			backup.BinLogEnd,
		}

		e.backupStartTimestamp.WithLabelValues(labels...).Set(float64(backup.StartLocalTime.Unix()))
		e.backupFinishTimestamp.WithLabelValues(labels...).Set(float64(backup.StopLocalTime.Unix()))

		// Set size metrics
		sizeLabels := []string{
			backup.BackupName,
			backupType,
			backup.Hostname,
		}
		e.backupUncompressedSize.WithLabelValues(sizeLabels...).Set(float64(backup.UncompressedSize))
		e.backupCompressedSize.WithLabelValues(sizeLabels...).Set(float64(backup.CompressedSize))

		// Set duration metric
		durationLabels := []string{
			backup.BackupName,
			backupType,
		}
		e.backupDuration.WithLabelValues(durationLabels...).Set(backup.GetBackupDuration())
	}

	// Set backup counts by type
	e.backupCount.WithLabelValues("full").Set(float64(fullCount))
	e.backupCount.WithLabelValues("incremental").Set(float64(incrementalCount))

	log.Printf("Updated metrics for %d backups (%d full, %d incremental)", len(backups), fullCount, incrementalCount)
}

// updateBinlogMetrics updates binlog-related metrics
func (e *MySQLWalgExporter) updateBinlogMetrics(binlogs []MySQLBinlogInfo) {
	if len(binlogs) == 0 {
		e.binlogCount.Set(0)
		e.binlogLatestTimestamp.Set(0)
		e.binlogTotalSize.Set(0)
		return
	}

	e.binlogCount.Set(float64(len(binlogs)))

	// Find latest binlog and calculate total size
	var latestTime time.Time
	var totalSize int64

	for _, binlog := range binlogs {
		if binlog.ModifiedTime.After(latestTime) {
			latestTime = binlog.ModifiedTime
		}
		totalSize += binlog.Size
	}

	if !latestTime.IsZero() {
		e.binlogLatestTimestamp.Set(float64(latestTime.Unix()))
	}
	e.binlogTotalSize.Set(float64(totalSize))

	log.Printf("Updated metrics for %d binlogs (total size: %d bytes)", len(binlogs), totalSize)
}

// checkStorageAliveness checks if the storage backend is accessible
func (e *MySQLWalgExporter) checkStorageAliveness() {
	start := time.Now()

	// Set a reasonable timeout for storage check
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try a simple WAL-G command to test storage connectivity
	var cmd *exec.Cmd
	if e.walgConfigPath != "" {
		cmd = exec.CommandContext(ctx, e.walgPath, "st", "check", "read", "--config", e.walgConfigPath)
	} else {
		cmd = exec.CommandContext(ctx, e.walgPath, "st", "check", "read")
	}

	err := cmd.Run()
	latency := time.Since(start).Seconds()

	// Set latency regardless of success/failure
	e.storageLatency.Set(latency)

	if err != nil {
		log.Printf("Storage aliveness check failed: %v", err)
		e.storageAlive.Set(0)
		e.errors.WithLabelValues("storage-check", "connectivity_failed").Inc()
	} else {
		e.storageAlive.Set(1)
	}
}
