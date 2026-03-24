package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

const (
	exporterBinary = "./walg-exporter-test"
	listenAddress  = "127.0.0.1:9352" // Use a different port for tests
)

// TestMain handles building the binary and cleanup.
func TestMain(m *testing.M) {
	// Build the exporter binary for testing
	buildCmd := exec.Command("go", "build", "-o", exporterBinary, ".")
	if err := buildCmd.Run(); err != nil {
		fmt.Printf("Failed to build exporter binary: %v\n", err)
		os.Exit(1)
	}

	// Make mock script executable
	if err := os.Chmod("./mock-wal-g.sh", 0755); err != nil {
		fmt.Printf("Failed to make mock script executable: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	os.Remove(exporterBinary)

	os.Exit(code)
}

func runExporter(t *testing.T, mockWalgPath string) func() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	args := []string{
		"-web.listen-address", listenAddress,
		"-walg.path", mockWalgPath,
		"-backup-list.scrape-interval", "2s", // Fast interval for testing
		"-wal-verify.scrape-interval", "2s",
		"-storage-check.scrape-interval", "2s",
	}

	cmd := exec.CommandContext(ctx, exporterBinary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // To kill the whole process group

	err := cmd.Start()
	require.NoError(t, err, "Failed to start exporter process")

	// Wait a bit for the server to start and perform an initial scrape
	time.Sleep(1 * time.Second)

	// Check if it's running
	_, err = http.Get("http://" + listenAddress + "/metrics")
	require.NoError(t, err, "Exporter HTTP server did not start in time")

	return func() {
		// Kill the process group to ensure the exporter and any children are terminated
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		cancelFunc()
		_ = cmd.Wait() // Wait for the process to exit
	}
}

func TestBackupMetricsIntegration(t *testing.T) {
	// Setup: Run the exporter with the mock wal-g script
	cancel := runExporter(t, "./mock-wal-g.sh")
	defer cancel()

	// Action: Scrape the /metrics endpoint
	resp, err := http.Get("http://" + listenAddress + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Assertions: Parse and check metrics
	// Initialize the parser with the Legacy validation scheme
	parser := expfmt.NewTextParser(model.LegacyValidation)
	metricFamilies, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	require.NoError(t, err)

	// Check for walg_backups count
	backupCountMetric := metricFamilies["walg_backups"]
	require.NotNil(t, backupCountMetric)
	foundFull, foundDelta := false, false
	for _, m := range backupCountMetric.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "backup_type" {
				if l.GetValue() == "full" {
					require.Equal(t, 7.0, m.GetGauge().GetValue(), "Expected 7 full backups")
					foundFull = true
				}
				if l.GetValue() == "delta" {
					require.Equal(t, 4.0, m.GetGauge().GetValue(), "Expected 4 delta backups")
					foundDelta = true
				}
			}
		}
	}
	require.True(t, foundFull, "Metric for full backups not found")
	require.True(t, foundDelta, "Metric for delta backups not found")

	// Check for a specific backup's compressed size
	compressedSizeMetric := metricFamilies["walg_backup_compressed_size_bytes"]
	require.NotNil(t, compressedSizeMetric)
	for _, m := range compressedSizeMetric.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "backup_name" && l.GetValue() == "base_000000430000038500000003" {
				require.Equal(t, 927914660.0, m.GetGauge().GetValue(), "Incorrect compressed size for full backup")
			}
			if l.GetName() == "backup_name" && l.GetValue() == "base_0000004300000388000000BB_D_0000004300000388000000AC" {
				require.Equal(t, 209052691.0, m.GetGauge().GetValue(), "Incorrect compressed size for delta backup")
			}
		}
	}

	// Check for walg_backup_info with delta_origin
	backupInfoMetric := metricFamilies["walg_backup_info"]
	require.NotNil(t, backupInfoMetric)
	for _, m := range backupInfoMetric.GetMetric() {
		var backupName, deltaOrigin string
		for _, l := range m.GetLabel() {
			if l.GetName() == "backup_name" {
				backupName = l.GetValue()
			}
			if l.GetName() == "delta_origin" {
				deltaOrigin = l.GetValue()
			}
		}
		if backupName == "base_0000004300000388000000BB_D_0000004300000388000000AC" {
			require.Equal(t, "base_0000004300000388000000AC_D_000000430000038500000003", deltaOrigin, "Incorrect delta_origin for delta backup")
		}
		if backupName == "base_0000004300000388000000AC_D_000000430000038500000003" {
			require.Equal(t, "base_000000430000038500000003", deltaOrigin, "Incorrect delta_origin for delta backup")
		}
	}
}

func TestWalVerifyMetricsIntegration(t *testing.T) {
	// Setup: Run the exporter with the mock wal-g script
	cancel := runExporter(t, "./mock-wal-g.sh")
	defer cancel()

	// Action: Scrape the /metrics endpoint
	resp, err := http.Get("http://" + listenAddress + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Assertions: Parse and check metrics
	// Initialize the parser with the Legacy validation scheme
	parser := expfmt.NewTextParser(model.LegacyValidation)
	metricFamilies, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	require.NoError(t, err)

	// Check walg_wal_integrity_status
	integrityMetric := metricFamilies["walg_wal_integrity_status"]
	require.NotNil(t, integrityMetric)
	for _, m := range integrityMetric.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "timeline_id" && l.GetValue() == "67" {
				require.Equal(t, 0.0, m.GetGauge().GetValue(), "Expected timeline 67 to be MISSING (0)")
			}
			if l.GetName() == "timeline_id" && l.GetValue() == "68" {
				require.Equal(t, 1.0, m.GetGauge().GetValue(), "Expected timeline 68 to be FOUND (1)")
			}
		}
	}

	// Check walg_wal_verify_status
	verifyMetric := metricFamilies["walg_wal_verify_status"]
	require.NotNil(t, verifyMetric)
	for _, m := range verifyMetric.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == "operation" && l.GetValue() == "integrity" {
				require.Equal(t, 0.0, m.GetGauge().GetValue(), "Expected integrity to be FAILURE (0)")
			}
			if l.GetName() == "operation" && l.GetValue() == "timeline" {
				require.Equal(t, 1.0, m.GetGauge().GetValue(), "Expected timeline to be OK (1)")
			}
		}
	}
}
