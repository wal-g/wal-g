package limiters

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"golang.org/x/time/rate"
)

// StartDiskRateLimitWatcher watches configFilePath for changes to settingKey and applies
// any new positive value to DiskLimiter (via SetDiskRateLimit) without requiring a restart.
// It is a no-op if configFilePath is empty, since there is nothing to watch in that case.
//
// A private viper instance is used instead of the global one: the global instance is read
// concurrently by many goroutines throughout a backup-push run, and viper has no internal
// locking, so re-reading it here would race with those reads.
func StartDiskRateLimitWatcher(configFilePath, settingKey string, burstPad int64) {
	if configFilePath == "" {
		tracelog.InfoLogger.Println(
			"Disk rate limit live reload disabled: no config file in use (value can only be changed by restarting WAL-G).")
		return
	}

	v := viper.New()
	v.SetConfigFile(configFilePath)

	applyDiskRateLimitFrom := func(v *viper.Viper) {
		newLimit := v.GetInt64(settingKey)
		if newLimit <= 0 {
			tracelog.WarningLogger.Printf("Disk rate limit watcher: ignoring non-positive or unset value for %s", settingKey)
			return
		}
		SetDiskRateLimit(rate.Limit(newLimit), int(newLimit+burstPad))
		tracelog.InfoLogger.Printf("Disk rate limit updated dynamically to %d bytes/s", newLimit)
	}

	if err := v.ReadInConfig(); err != nil {
		tracelog.WarningLogger.Printf("Disk rate limit watcher: failed to read config file %s: %v", configFilePath, err)
	} else {
		applyDiskRateLimitFrom(v)
	}

	v.OnConfigChange(func(_ fsnotify.Event) { applyDiskRateLimitFrom(v) })
	v.WatchConfig()
}
