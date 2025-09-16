package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// WalgExporter implements the Prometheus Collector interface
type WalgExporter struct {
	walgPath       string
	scrapeInterval time.Duration

	// Metrics
	walTimestamp    *prometheus.GaugeVec
	lsnLag          *prometheus.GaugeVec
	pitrWindow      prometheus.Gauge
	errors          *prometheus.CounterVec
	walIntegrity    *prometheus.GaugeVec
	backupCount     *prometheus.GaugeVec
	backupTimestamp *prometheus.GaugeVec
	scrapeDuration  prometheus.Gauge
	scrapeErrors    prometheus.Counter

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
}

// Helper function to convert uint64 LSN to string format (X/Y)
func formatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// Helper method to get backup type
func (b *BackupInfo) GetBackupType() string {
	// In real wal-g, determine backup type based on backup name or other fields
	// For now, assume base backups start with "base_" and others are delta
	if len(b.BackupName) >= 5 && b.BackupName[:5] == "base_" {
		return "full"
	}
	return "delta"
}

// Helper method to check if backup is full
func (b *BackupInfo) IsFullBackup() bool {
	return b.GetBackupType() == "full"
}

// TimelineInfo represents timeline information from wal-show --detailed-json
// This matches the actual structure returned by wal-g wal-show --detailed-json
type TimelineInfo struct {
	ID               uint32   `json:"id"`
	ParentID         uint32   `json:"parent_id"`
	SwitchPointLsn   uint64   `json:"switch_point_lsn"` // LSN is serialized as uint64 in JSON
	StartSegment     string   `json:"start_segment"`
	EndSegment       string   `json:"end_segment"`
	SegmentsCount    int      `json:"segments_count"`
	MissingSegments  []string `json:"missing_segments"`
	SegmentRangeSize uint64   `json:"segment_range_size"`
	Status           string   `json:"status"`
}

// NewWalgExporter creates a new WAL-G exporter
func NewWalgExporter(walgPath string, scrapeInterval time.Duration) (*WalgExporter, error) {
	return &WalgExporter{
		walgPath:       walgPath,
		scrapeInterval: scrapeInterval,

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
				Name: "walg_backup_count",
				Help: "Number of backups by type",
			},
			[]string{"backup_type"},
		),

		backupTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_timestamp",
				Help: "Timestamp of backup",
			},
			[]string{"backup_name", "backup_type", "wal_file", "start_lsn", "finish_lsn", "permanent"},
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
	e.backupTimestamp.Describe(ch)
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
	e.backupTimestamp.Collect(ch)
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

	// Get backup information
	backups, err := e.getBackupInfo()
	if err != nil {
		log.Printf("Error getting backup info: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		return
	}

	// Get WAL information
	timelineInfos, err := e.getWalInfo()
	if err != nil {
		log.Printf("Error getting WAL info: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("wal-show", "command_failed").Inc()
		return
	}

	// Check storage aliveness
	e.checkStorageAliveness()

	// Update backup metrics
	e.updateBackupMetrics(backups)

	// Update WAL metrics
	e.updateWalMetrics(timelineInfos)

	// Calculate PITR window
	e.updatePitrWindow(backups, timelineInfos)

	log.Printf("Metrics scrape completed in %v", time.Since(start))
}

// getBackupInfo executes wal-g backup-list --detail --json
func (e *WalgExporter) getBackupInfo() ([]BackupInfo, error) {
	cmd := exec.Command(e.walgPath, "backup-list", "--detail", "--json")
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

// getWalInfo executes wal-g wal-show --detailed-json
func (e *WalgExporter) getWalInfo() ([]TimelineInfo, error) {
	cmd := exec.Command(e.walgPath, "wal-show", "--detailed-json")
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
	e.backupTimestamp.Reset()

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

		// Labels for detailed backup information
		labels := []string{
			backup.BackupName,
			backupType,
			backup.WalFileName,
			formatLSN(backup.StartLSN),  // Convert uint64 LSN to string format
			formatLSN(backup.FinishLSN), // Convert uint64 LSN to string format
			permanent,
		}

		// Set timestamp for this specific backup
		e.backupTimestamp.WithLabelValues(labels...).Set(float64(backup.Time.Unix()))
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
	cmd := exec.CommandContext(ctx, e.walgPath, "st", "ls")

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
