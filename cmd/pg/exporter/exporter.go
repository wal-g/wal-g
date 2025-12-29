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

// WalgExporter implements the Prometheus Collector interface
type WalgExporter struct {
	walgPath       string
	walgConfigPath string
	scrapeInterval time.Duration

	// Metrics
	walTimestamp           *prometheus.GaugeVec
	lsnLag                 *prometheus.GaugeVec
	pitrWindow             prometheus.Gauge
	errors                 *prometheus.CounterVec
	walIntegrity           *prometheus.GaugeVec
	backupCount            *prometheus.GaugeVec
	backupStartTimestamp   *prometheus.GaugeVec
	backupFinishTimestamp  *prometheus.GaugeVec
	backupUncompressedSize *prometheus.GaugeVec
	backupCompressedSize   *prometheus.GaugeVec
	scrapeDuration         prometheus.Gauge
	scrapeErrors           prometheus.Counter

	// Storage aliveness metrics
	storageAlive   prometheus.Gauge
	storageLatency prometheus.Gauge

	// Internal state
	lastScrape time.Time
}

// BackupInfo represents backup information from backup-list --detail --json
// Updated to match real wal-g output format
type BackupInfo struct {
	BackupName       string      `json:"backup_name"`
	Time             time.Time   `json:"time"`
	WalFileName      string      `json:"wal_file_name"`
	StorageName      string      `json:"storage_name"`
	StartTime        time.Time   `json:"start_time"`
	FinishTime       time.Time   `json:"finish_time"`
	DateFmt          string      `json:"date_fmt"`
	Hostname         string      `json:"hostname"`
	DataDir          string      `json:"data_dir"`
	PgVersion        int         `json:"pg_version"`
	StartLSN         uint64      `json:"start_lsn"`  // Real wal-g returns this as number
	FinishLSN        uint64      `json:"finish_lsn"` // Real wal-g returns this as number
	IsPermanent      bool        `json:"is_permanent"`
	SystemIdentifier uint64      `json:"system_identifier"`
	UncompressedSize int64       `json:"uncompressed_size"`
	CompressedSize   int64       `json:"compressed_size"`
	UserData         interface{} `json:"user_data,omitempty"`
	// Note: Real WAL-G doesn't include is_full field, we determine it from backup name
}

// Helper method to get backup type
func (b *BackupInfo) GetBackupType() string {
	// WAL-G doesn't include is_full in JSON output, so we determine backup type
	// from the backup name using the actual WAL-G naming convention:
	// - Incremental backups have "_D_" in their name (added during delta backup creation)
	// - Full backups don't have "_D_" in their name
	if b.IsFullBackup() {
		return "full"
	}
	return "delta"
}

// Helper method to check if backup is full
func (b *BackupInfo) IsFullBackup() bool {
	// In WAL-G, incremental/delta backups get "_D_" suffix added to their name
	// (see backup_push_handler.go line 285: bh.CurBackupInfo.Name = bh.CurBackupInfo.Name + "_D_" + ...)
	// So if backup name contains "_D_", it's incremental; otherwise it's full
	return !strings.Contains(b.BackupName, "_D_")
}

// GetBaseBackupName extracts the base backup name for delta backups
// For delta backups with format "base_XXXXX_D_YYYYY", this returns "base_YYYYY"
// For full backups, this returns empty string
func (b *BackupInfo) GetBaseBackupName() string {
	if b.IsFullBackup() {
		return "" // Full backups don't have a base backup
	}

	// Find the "_D_" pattern in the backup name
	deltaIndex := strings.Index(b.BackupName, "_D_")
	if deltaIndex == -1 {
		return "" // Shouldn't happen if IsFullBackup() returned false
	}

	// Extract the part after "_D_" which contains the base backup identifier
	baseIdentifier := b.BackupName[deltaIndex+3:] // +3 to skip "_D_"

	// The base backup name follows the pattern "base_" + identifier
	return "base_" + baseIdentifier
}

// GetStartLSN converts the uint64 StartLSN to postgres.LSN type
func (b *BackupInfo) GetStartLSN() LSN {
	return LSN(b.StartLSN)
}

// GetFinishLSN converts the uint64 FinishLSN to postgres.LSN type
func (b *BackupInfo) GetFinishLSN() LSN {
	return LSN(b.FinishLSN)
}

// TimelineInfo represents timeline information from wal-show --detailed-json
// This matches the actual structure returned by wal-g wal-show --detailed-json
type TimelineInfo struct {
	ID               uint32       `json:"id"`
	ParentID         uint32       `json:"parent_id"`
	SwitchPointLsn   uint64       `json:"switch_point_lsn"` // LSN is serialized as uint64 in JSON
	StartSegment     string       `json:"start_segment"`
	EndSegment       string       `json:"end_segment"`
	SegmentsCount    int          `json:"segments_count"`
	MissingSegments  []string     `json:"missing_segments"`
	SegmentRangeSize uint64       `json:"segment_range_size"`
	Status           string       `json:"status"`
	BackupInfo       []BackupInfo `json:"backups"`
}

// GetSwitchPointLSN converts the uint64 SwitchPointLsn to postgres.LSN type
func (t *TimelineInfo) GetSwitchPointLSN() LSN {
	return LSN(t.SwitchPointLsn)
}

// NewWalgExporter creates a new WAL-G exporter
func NewWalgExporter(walgPath string, scrapeInterval time.Duration, walgConfigPath string) (*WalgExporter, error) {
	return &WalgExporter{
		walgPath:       walgPath,
		scrapeInterval: scrapeInterval,
		walgConfigPath: walgConfigPath,
		walTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_wal_timestamp",
				Help: "Timestamp of last wal-push operation",
			},
			[]string{"timeline"},
		),

		lsnLag: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_lsn_lag_bytes",
				Help: "LSN delta lag in bytes",
			},
			[]string{"timeline"},
		),

		pitrWindow: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_pitr_window_seconds",
				Help: "Point-in-time recovery window size in seconds",
			},
		),

		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "walg_errors_total",
				Help: "Total number of WAL-G errors",
			},
			[]string{"operation", "error_type"},
		),

		walIntegrity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_wal_integrity_status",
				Help: "WAL integrity status (1 = OK, 0 = ERROR)",
			},
			[]string{"timeline"},
		),

		backupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backups",
				Help: "Number of backups by type",
			},
			[]string{"backup_type"},
		),

		backupStartTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_start_timestamp",
				Help: "Start timestamp of backup",
			},
			[]string{"backup_name", "backup_type", "wal_file", "start_lsn", "finish_lsn", "permanent", "base_backup"},
		),

		backupFinishTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_finish_timestamp",
				Help: "Finish timestamp of backup",
			},
			[]string{"backup_name", "backup_type", "wal_file", "start_lsn", "finish_lsn", "permanent", "base_backup"},
		),

		backupUncompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_uncompressed_size_bytes",
				Help: "Uncompressed size of the backup in bytes.",
			},
			[]string{"backup_name", "backup_type", "wal_file", "start_lsn", "finish_lsn", "permanent", "base_backup"},
		),

		backupCompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_compressed_size_bytes",
				Help: "Compressed size of the backup in bytes.",
			},
			[]string{"backup_name", "backup_type", "wal_file", "start_lsn", "finish_lsn", "permanent", "base_backup"},
		),

		scrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_scrape_duration_seconds",
				Help: "Duration of the last scrape",
			},
		),

		scrapeErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "walg_scrape_errors_total",
				Help: "Total number of scrape errors",
			},
		),

		storageAlive: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_storage_alive",
				Help: "Storage connectivity status (1 = alive, 0 = dead)",
			},
		),

		storageLatency: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_storage_latency_seconds",
				Help: "Storage operation latency in seconds",
			},
		),
	}, nil
}

// Describe implements the Prometheus Collector interface
func (e *WalgExporter) Describe(ch chan<- *prometheus.Desc) {
	e.walTimestamp.Describe(ch)
	e.lsnLag.Describe(ch)
	e.pitrWindow.Describe(ch)
	e.errors.Describe(ch)
	e.walIntegrity.Describe(ch)
	e.backupCount.Describe(ch)
	e.backupStartTimestamp.Describe(ch)
	e.backupFinishTimestamp.Describe(ch)
	e.backupUncompressedSize.Describe(ch)
	e.backupCompressedSize.Describe(ch)
	e.scrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.storageAlive.Describe(ch)
	e.storageLatency.Describe(ch)
}

// Collect implements the Prometheus Collector interface
func (e *WalgExporter) Collect(ch chan<- prometheus.Metric) {
	e.walTimestamp.Collect(ch)
	e.lsnLag.Collect(ch)
	e.pitrWindow.Collect(ch)
	e.errors.Collect(ch)
	e.walIntegrity.Collect(ch)
	e.backupCount.Collect(ch)
	e.backupStartTimestamp.Collect(ch)
	e.backupFinishTimestamp.Collect(ch)
	e.backupUncompressedSize.Collect(ch)
	e.backupCompressedSize.Collect(ch)
	e.scrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.storageAlive.Collect(ch)
	e.storageLatency.Collect(ch)
}

// Start begins the metrics collection loop
func (e *WalgExporter) Start(ctx context.Context) {
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
func (e *WalgExporter) scrapeMetrics() {
	start := time.Now()
	defer func() {
		e.scrapeDuration.Set(time.Since(start).Seconds())
		e.lastScrape = time.Now()
	}()

	log.Printf("Scraping WAL-G metrics...")

	// Get fresh backup list directly 
	backups, err := e.getBackupsDirect()
	if err != nil {
		log.Printf("Error getting backups: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		return
	}

	// Get WAL timeline info (for WAL metrics, not for backup data)
	timelineInfos, err := e.getWalInfo()
	if err != nil {
		log.Printf("Error getting WAL info: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("wal-show", "command_failed").Inc()
		// Don't return - we still have backup data from backup-list
		timelineInfos = []TimelineInfo{}
	}

	// Check storage aliveness
	e.checkStorageAliveness()

	// Update backup metrics with FRESH data from backup-list
	e.updateBackupMetrics(backups)

	// Update WAL metrics
	e.updateWalMetrics(timelineInfos)

	// Calculate PITR window
	e.updatePitrWindow(backups, timelineInfos)

	log.Printf("Metrics scrape completed in %v", time.Since(start))
}

// getWalInfo executes wal-g wal-show --detailed-json
func (e *WalgExporter) getWalInfo() ([]TimelineInfo, error) {
	var cmd *exec.Cmd

	if e.walgConfigPath != "" {
		cmd = exec.Command(e.walgPath, "wal-show", "--detailed-json", "--config", e.walgConfigPath)
	} else {
		cmd = exec.Command(e.walgPath, "wal-show", "--detailed-json")
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute wal-show: %w", err)
	}

	var timelineInfos []TimelineInfo
	if err := json.Unmarshal(output, &timelineInfos); err != nil {
		return nil, fmt.Errorf("failed to parse wal-show output: %w", err)
	}

	return timelineInfos, nil
}

// updateBackupMetrics updates backup-related metrics with detailed labels
func (e *WalgExporter) updateBackupMetrics(backups []BackupInfo) {
	// Reset metrics
	e.backupCount.Reset()
	e.backupStartTimestamp.Reset()
	e.backupFinishTimestamp.Reset()
	e.backupUncompressedSize.Reset()
	e.backupCompressedSize.Reset()

	fullCount, deltaCount := 0, 0

	// Create detailed metrics for each backup
	for _, backup := range backups {
		backupType := backup.GetBackupType()
		if backup.IsFullBackup() {
			fullCount++
		} else {
			deltaCount++
		}

		permanent := "false"
		if backup.IsPermanent {
			permanent = "true"
		}

		// Get base backup name for delta backups (empty for full backups)
		baseBackupName := backup.GetBaseBackupName()

		// Labels for detailed backup information
		labels := []string{
			backup.BackupName,
			backupType,
			backup.WalFileName,
			backup.GetStartLSN().String(),  // Convert uint64 LSN to string format
			backup.GetFinishLSN().String(), // Convert uint64 LSN to string format
			permanent,
			baseBackupName, // Base backup name for delta backups, empty for full backups
		}

		// Set start and finish timestamps for this specific backup
		e.backupStartTimestamp.WithLabelValues(labels...).Set(float64(backup.StartTime.Unix()))
		e.backupFinishTimestamp.WithLabelValues(labels...).Set(float64(backup.FinishTime.Unix()))

		// Set the size metrics for the specific backup
		e.backupUncompressedSize.WithLabelValues(labels...).Set(float64(backup.UncompressedSize))
		e.backupCompressedSize.WithLabelValues(labels...).Set(float64(backup.CompressedSize))
	}

	// Set backup counts by type
	e.backupCount.WithLabelValues("full").Set(float64(fullCount))
	e.backupCount.WithLabelValues("delta").Set(float64(deltaCount))
}

// updateWalMetrics updates WAL-related metrics
func (e *WalgExporter) updateWalMetrics(timelineInfos []TimelineInfo) {
	// Reset metrics
	e.walIntegrity.Reset()
	e.walTimestamp.Reset()

	// Set WAL integrity status for each timeline
	for _, timeline := range timelineInfos {
		timelineStr := strconv.Itoa(int(timeline.ID))
		var status float64
		if timeline.Status == "OK" {
			status = 1
		} else {
			status = 0
		}
		e.walIntegrity.WithLabelValues(timelineStr).Set(status)
	}

	// TODO: Implement WAL timestamp and LSN lag calculations
	// This requires more complex logic to determine the current WAL position
	// and get timestamps from the latest WAL segments
	// When implemented, use: e.walTimestamp.WithLabelValues(timelineStr).Set(float64(walTime.Unix()))
}

// getBackupsDirect executes wal-g backup-list --detail --json
func (e *WalgExporter) getBackupsDirect() ([]BackupInfo, error) {
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

	var backups []BackupInfo
	if err := json.Unmarshal(output, &backups); err != nil {
		return nil, fmt.Errorf("failed to parse backup-list output: %w", err)
	}

	return backups, nil
}

// updatePitrWindow calculates and updates the PITR window size
func (e *WalgExporter) updatePitrWindow(backups []BackupInfo, timelineInfos []TimelineInfo) {
	if len(backups) == 0 {
		e.pitrWindow.Set(0)
		return
	}

	// Find the oldest backup
	var oldestBackup time.Time
	for _, backup := range backups {
		if oldestBackup.IsZero() || backup.Time.Before(oldestBackup) {
			oldestBackup = backup.Time
		}
	}

	// PITR window is from the oldest backup to now
	// In a real implementation, this should be to the latest WAL segment
	pitrWindow := time.Since(oldestBackup).Seconds()
	e.pitrWindow.Set(pitrWindow)
}

// checkStorageAliveness checks if the storage backend is accessible
func (e *WalgExporter) checkStorageAliveness() {
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
