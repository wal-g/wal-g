package limiters_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/limiters"
	"golang.org/x/time/rate"
)

const diskRateLimitWatcherTestSettingKey = "WALG_DISK_RATE_LIMIT"

func writeDiskRateLimitConfig(t *testing.T, path string, contents string) {
	t.Helper()
	assert.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}

func waitForDiskLimiterValue(t *testing.T, expected int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if limiters.DiskLimiter != nil && limiters.DiskLimiter.Limit() == rate.Limit(expected) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("disk limiter did not reach expected limit %d in time", expected)
}

func TestStartDiskRateLimitWatcher_NoConfigFile(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	limiters.StartDiskRateLimitWatcher("", diskRateLimitWatcherTestSettingKey, 0)

	assert.Nil(t, limiters.DiskLimiter)
}

func TestStartDiskRateLimitWatcher_AppliesChangesFromFile(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	configPath := filepath.Join(t.TempDir(), "walg.json")
	writeDiskRateLimitConfig(t, configPath, `{"WALG_DISK_RATE_LIMIT": 1000}`)

	limiters.StartDiskRateLimitWatcher(configPath, diskRateLimitWatcherTestSettingKey, 0)
	waitForDiskLimiterValue(t, 1000)

	writeDiskRateLimitConfig(t, configPath, `{"WALG_DISK_RATE_LIMIT": 5000}`)
	waitForDiskLimiterValue(t, 5000)
}

func TestStartDiskRateLimitWatcher_IgnoresNonPositiveValue(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	configPath := filepath.Join(t.TempDir(), "walg.json")
	writeDiskRateLimitConfig(t, configPath, `{"WALG_DISK_RATE_LIMIT": 1000}`)

	limiters.StartDiskRateLimitWatcher(configPath, diskRateLimitWatcherTestSettingKey, 0)
	waitForDiskLimiterValue(t, 1000)

	writeDiskRateLimitConfig(t, configPath, `{"WALG_DISK_RATE_LIMIT": -1}`)
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, rate.Limit(1000), limiters.DiskLimiter.Limit())
}
