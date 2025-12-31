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

	// Scrape intervals
	backupScrapeInterval  time.Duration
	verifyScrapeInterval  time.Duration
	storageScrapeInterval time.Duration

	// Metrics
	pitrWindow   prometheus.Gauge
	errors       *prometheus.CounterVec
	scrapeErrors prometheus.Counter

	// Metrics of backups
	backupCount            *prometheus.GaugeVec
	backupInfo             *prometheus.GaugeVec
	backupStartTimestamp   *prometheus.GaugeVec
	backupFinishTimestamp  *prometheus.GaugeVec
	backupUncompressedSize *prometheus.GaugeVec
	backupCompressedSize   *prometheus.GaugeVec
	backupScrapeDuration   prometheus.Gauge

	// Metrics of wal-verify
	walVerifyCheck       *prometheus.GaugeVec
	walIntegrity         *prometheus.GaugeVec
	verifyScrapeDuration prometheus.Gauge

	// Storage aliveness metrics
	storageAlive   prometheus.Gauge
	storageLatency prometheus.Gauge
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

// WalVerifyResponse represents information from wal-verify integrity timeline --json
type WalVerifyResponse struct {
	Integrity IntegrityData `json:"integrity"`
	Timeline  TimelineData  `json:"timeline"`
}

// IntegrityData represents the inner integrity object
type IntegrityData struct {
	Status  string            `json:"status"`
	Details []IntegrityDetail `json:"details"`
}

// IntegrityDetail represents the individual items in the integrity details array
type IntegrityDetail struct {
	TimelineID    int    `json:"timeline_id"`
	StartSegment  string `json:"start_segment"`
	EndSegment    string `json:"end_segment"`
	SegmentsCount int    `json:"segments_count"`
	Status        string `json:"status"`
}

// TimelineData represents the inner timeline object
type TimelineData struct {
	Status  string         `json:"status"`
	Details TimelineDetail `json:"details"`
}

// TimelineDetail represents the timeline details object
type TimelineDetail struct {
	CurrentTimelineID        int `json:"current_timeline_id"`
	HighestStorageTimelineID int `json:"highest_storage_timeline_id"`
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

// GetDeltaOriginName extracts the parent backup name for delta backups
func (b *BackupInfo) GetDeltaOriginName(backups []BackupInfo) string {
	if b.IsFullBackup() {
		return "" // Full backups don't have a base backup
	}

	// Find the "_D_" pattern in the backup name
	deltaIndex := strings.Index(b.BackupName, "_D_")
	if deltaIndex == -1 || len(b.BackupName) <= deltaIndex+3 {
		return ""
	}

	// Extract the part after "_D_" which contains the base backup identifier
	expectedParent := "base_" + b.BackupName[deltaIndex+3:] // +3 to skip "_D_"
	for _, candidate := range backups {
		if b.BackupName != candidate.BackupName && strings.HasPrefix(candidate.BackupName, expectedParent) {
			return candidate.BackupName
		}
	}

	return ""
}

// GetStartLSN converts the uint64 StartLSN to postgres.LSN type
func (b *BackupInfo) GetStartLSN() LSN {
	return LSN(b.StartLSN)
}

// GetFinishLSN converts the uint64 FinishLSN to postgres.LSN type
func (b *BackupInfo) GetFinishLSN() LSN {
	return LSN(b.FinishLSN)
}

// NewWalgExporter creates a new WAL-G exporter
func NewWalgExporter(walgPath string, backupScrapeInterval time.Duration, verifyScrapeInterval time.Duration, storageScrapeInterval time.Duration, walgConfigPath string) (*WalgExporter, error) {
	return &WalgExporter{
		walgPath:              walgPath,
		backupScrapeInterval:  backupScrapeInterval,
		verifyScrapeInterval:  verifyScrapeInterval,
		storageScrapeInterval: storageScrapeInterval,
		walgConfigPath:        walgConfigPath,

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

		walVerifyCheck: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_wal_verify_status",
				Help: "WAL verify status (1 = OK, 0 = FAILURE, 2 = WARNING, -1 = UNKNOWN)",
			},
			[]string{"operation"},
		),

		walIntegrity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_wal_integrity_status",
				Help: "WAL integrity status (1 = FOUND, 0 = MISSING)",
			},
			[]string{"timeline_id", "timeline_hex"},
		),

		backupCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backups",
				Help: "Number of backups by type",
			},
			[]string{"backup_type"},
		),

		backupInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_info",
				Help: "Information about stored backups. Value is always 1.",
			},
			[]string{"backup_name", "backup_type", "wal_file", "pg_version", "start_lsn", "finish_lsn", "is_permanent", "delta_origin"},
		),

		backupStartTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_start_timestamp",
				Help: "Start time of the backup (Unix timestamp).",
			},
			[]string{"backup_name"},
		),

		backupFinishTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_finish_timestamp",
				Help: "Finish time of the backup (Unix timestamp).",
			},
			[]string{"backup_name"},
		),

		backupUncompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_uncompressed_size_bytes",
				Help: "Uncompressed size of the backup in bytes.",
			},
			[]string{"backup_name"},
		),

		backupCompressedSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "walg_backup_compressed_size_bytes",
				Help: "Compressed size of the backup in bytes.",
			},
			[]string{"backup_name"},
		),

		backupScrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_backup_list_duration_seconds",
				Help: "Time taken to execute 'backup-list' during the last collector run.",
			},
		),

		verifyScrapeDuration: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "walg_wal_verify_duration_seconds",
				Help: "Time taken to execute 'wal-verify' during the last collector run.",
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
				Name: "walg_storage_up",
				Help: "Storage connectivity status (1 = up, 0 = down)",
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
	e.pitrWindow.Describe(ch)
	e.errors.Describe(ch)
	e.walVerifyCheck.Describe(ch)
	e.walIntegrity.Describe(ch)
	e.backupCount.Describe(ch)
	e.backupInfo.Describe(ch)
	e.backupStartTimestamp.Describe(ch)
	e.backupFinishTimestamp.Describe(ch)
	e.backupUncompressedSize.Describe(ch)
	e.backupCompressedSize.Describe(ch)
	e.backupScrapeDuration.Describe(ch)
	e.verifyScrapeDuration.Describe(ch)
	e.scrapeErrors.Describe(ch)
	e.storageAlive.Describe(ch)
	e.storageLatency.Describe(ch)
}

// Collect implements the Prometheus Collector interface
func (e *WalgExporter) Collect(ch chan<- prometheus.Metric) {
	e.pitrWindow.Collect(ch)
	e.errors.Collect(ch)
	e.walVerifyCheck.Collect(ch)
	e.walIntegrity.Collect(ch)
	e.backupCount.Collect(ch)
	e.backupInfo.Collect(ch)
	e.backupStartTimestamp.Collect(ch)
	e.backupFinishTimestamp.Collect(ch)
	e.backupUncompressedSize.Collect(ch)
	e.backupCompressedSize.Collect(ch)
	e.backupScrapeDuration.Collect(ch)
	e.verifyScrapeDuration.Collect(ch)
	e.scrapeErrors.Collect(ch)
	e.storageAlive.Collect(ch)
	e.storageLatency.Collect(ch)
}

// Start begins the metrics collection loop
func (e *WalgExporter) Start(ctx context.Context) {
	tickerStorage := time.NewTicker(e.storageScrapeInterval)
	defer tickerStorage.Stop()
	tickerBackup := time.NewTicker(e.backupScrapeInterval)
	defer tickerBackup.Stop()
	tickerWalVerify := time.NewTicker(e.verifyScrapeInterval)
	defer tickerWalVerify.Stop()

	// Initial scrape
	e.checkStorageAliveness()
	e.scrapeBackupMetrics()
	e.scrapeWalMetrics()

	log.Printf("Initial WAL-G metrics scrape completed; starting periodic collection")
	for {
		select {
		case <-ctx.Done():
			log.Println("Exporter context cancelled, stopping metrics collection")
			return
		case <-tickerStorage.C:
			e.checkStorageAliveness()
		case <-tickerBackup.C:
			e.scrapeBackupMetrics()
		case <-tickerWalVerify.C:
			e.scrapeWalMetrics()
		}
	}
}

// scrapeBackupMetrics collects backup metrics from WAL-G
func (e *WalgExporter) scrapeBackupMetrics() {
	start := time.Now()
	defer func() {
		e.backupScrapeDuration.Set(time.Since(start).Seconds())
	}()

	// Get backup information
	backups, err := e.getBackupInfo()
	if err != nil {
		log.Printf("Error getting backup info: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("backup-list", "command_failed").Inc()
		return
	}

	// Update backup metrics
	e.updateBackupMetrics(backups)

	// Calculate PITR window
	e.updatePitrWindow(backups)

	log.Printf("Metrics for backups scrape completed in %v", time.Since(start))
}

// scrapeWalMetrics collects wal metrics from WAL-G
func (e *WalgExporter) scrapeWalMetrics() {
	start := time.Now()
	defer func() {
		e.verifyScrapeDuration.Set(time.Since(start).Seconds())
	}()

	// Get WAL verify information
	verifyData, err := e.getWalVerify()
	if err != nil {
		log.Printf("Error getting WAL verify info: %v", err)
		e.scrapeErrors.Inc()
		e.errors.WithLabelValues("wal-verify", "command_failed").Inc()
		return
	}

	// Update WAL verify metrics
	e.updateWalMetrics(verifyData)

	log.Printf("Metrics for WALs verify scrape completed in %v", time.Since(start))
}

// getBackupInfo executes wal-g backup-list --detail --json
func (e *WalgExporter) getBackupInfo() ([]BackupInfo, error) {
	args := []string{"backup-list", "--detail", "--json"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}
	cmd := exec.Command(e.walgPath, args...)
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

// getWalVerify executes wal-g wal-verify integrity timeline --json
func (e *WalgExporter) getWalVerify() (*WalVerifyResponse, error) {
	args := []string{"wal-verify", "integrity", "timeline", "--json"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}
	cmd := exec.Command(e.walgPath, args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute wal-verify: %w", err)
	}

	var walVerifyResponse WalVerifyResponse
	if err := json.Unmarshal(output, &walVerifyResponse); err != nil {
		return nil, fmt.Errorf("failed to parse wal-verify output: %w", err)
	}

	return &walVerifyResponse, nil
}

// updateBackupMetrics updates backup-related metrics with detailed labels
func (e *WalgExporter) updateBackupMetrics(backups []BackupInfo) {
	// Reset metrics
	e.backupCount.Reset()
	e.backupInfo.Reset()
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

		isPermanent := strconv.FormatBool(backup.IsPermanent)

		// Get parent backup name for delta backups (empty for full backups)
		deltaOriginBackupName := backup.GetDeltaOriginName(backups)

		// Labels for detailed backup information
		labels := []string{
			backup.BackupName,              // backup_name
			backupType,                     // backup_type
			backup.WalFileName,             // wal_file
			strconv.Itoa(backup.PgVersion), // pg_version
			backup.GetStartLSN().String(),  // start_lsn
			backup.GetFinishLSN().String(), // finish_lsn
			isPermanent,                    // is_permanent
			deltaOriginBackupName,          // delta_origin
		}
		e.backupInfo.WithLabelValues(labels...).Set(1)

		// Set start and finish timestamps for this specific backup
		e.backupStartTimestamp.WithLabelValues(backup.BackupName).Set(float64(backup.StartTime.Unix()))
		e.backupFinishTimestamp.WithLabelValues(backup.BackupName).Set(float64(backup.FinishTime.Unix()))

		// Set the size metrics for the specific backup
		e.backupUncompressedSize.WithLabelValues(backup.BackupName).Set(float64(backup.UncompressedSize))
		e.backupCompressedSize.WithLabelValues(backup.BackupName).Set(float64(backup.CompressedSize))
	}

	// Set backup counts by type
	e.backupCount.WithLabelValues("full").Set(float64(fullCount))
	e.backupCount.WithLabelValues("delta").Set(float64(deltaCount))
}

func mapStatus(status string) float64 {
	switch status {
	case "OK":
		return 1.0
	case "WARNING":
		return 2.0
	case "FAILURE":
		return 0.0
	default:
		return -1.0 // "UNKNOWN" state
	}
}

// updateWalMetrics updates WAL-related metrics
func (e *WalgExporter) updateWalMetrics(verifyData *WalVerifyResponse) {
	// Reset metrics
	e.walVerifyCheck.Reset()
	e.walIntegrity.Reset()

	e.walVerifyCheck.WithLabelValues("integrity").Set(mapStatus(verifyData.Integrity.Status))
	e.walVerifyCheck.WithLabelValues("timeline").Set(mapStatus(verifyData.Timeline.Status))

	timelineStatusMap := make(map[int]bool)
	for _, data := range verifyData.Integrity.Details {
		id := data.TimelineID
		// If a single segment is not "FOUND", the entire timeline is marked failed (false)
		if data.Status != "FOUND" {
			timelineStatusMap[id] = false
			continue
		}
		// Initialize the timeline as true if we haven't seen it yet
		if _, exists := timelineStatusMap[id]; !exists {
			timelineStatusMap[id] = true
		}
	}

	for id, isTimelineOK := range timelineStatusMap {
		timelineStr := strconv.Itoa(id)
		// Also show the timeline in hex format
		timelineHex := fmt.Sprintf("%08x", id)

		statusTimeline := 0.0
		if isTimelineOK {
			statusTimeline = 1.0
		}

		e.walIntegrity.WithLabelValues(timelineStr, timelineHex).Set(statusTimeline)
	}
}

// updatePitrWindow calculates and updates the PITR window size
func (e *WalgExporter) updatePitrWindow(backups []BackupInfo) {
	if len(backups) == 0 {
		e.pitrWindow.Set(0)
		return
	}

	// Find earliest non-permanent backup (matches wal-verify behavior)
	var earliestEligibleBackup *BackupInfo

	for i := range backups {
		// Skip permanent backups
		if backups[i].IsPermanent {
			continue
		}

		// Update if this is the first eligible backup or older than the current find
		if earliestEligibleBackup == nil || backups[i].Time.Before(earliestEligibleBackup.Time) {
			earliestEligibleBackup = &backups[i]
		}
	}

	// If no backups or no eligible backups found, set window to 0
	if earliestEligibleBackup == nil {
		e.pitrWindow.Set(0)
		return
	}

	// Calculate window, ensuring we don't report negative time
	window := time.Since(earliestEligibleBackup.Time).Seconds()
	if window < 0 {
		window = 0
	}

	e.pitrWindow.Set(window)
}

// checkStorageAliveness checks if the storage backend is accessible
func (e *WalgExporter) checkStorageAliveness() {
	start := time.Now()

	// Set a reasonable timeout for storage check
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try a simple WAL-G command to test storage connectivity
	args := []string{"st", "check", "read"}
	if e.walgConfigPath != "" {
		args = append(args, "--config", e.walgConfigPath)
	}
	cmd := exec.CommandContext(ctx, e.walgPath, args...)

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
	log.Printf("Storage check completed in %v", time.Since(start))
}
