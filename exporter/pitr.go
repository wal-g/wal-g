package main

import (
	"time"
)

// PitrWindow represents the Point-in-Time Recovery window
type PitrWindow struct {
	OldestBackupTime time.Time
	NewestWalTime    time.Time
	WindowSeconds    float64
	IsValid          bool
}

// calculatePitrWindow calculates the PITR window based on backup and WAL information
func calculatePitrWindow(backups []BackupInfo, timelineInfos []TimelineInfo) *PitrWindow {
	window := &PitrWindow{
		IsValid: false,
	}

	if len(backups) == 0 {
		return window
	}

	// Find the oldest backup
	var oldestBackup time.Time
	for _, backup := range backups {
		if oldestBackup.IsZero() || backup.Time.Before(oldestBackup) {
			oldestBackup = backup.Time
		}
	}

	window.OldestBackupTime = oldestBackup

	// For simplicity, we'll use the current time as the newest WAL time
	// In a real implementation, you would parse the WAL segment information
	// to determine the actual newest WAL segment time
	newestWalTime := time.Now()
	if len(timelineInfos) > 0 {
		// Try to extract timestamp from WAL info if available
		// This is a simplified approach - in reality you'd parse WAL segment names
		newestWalTime = time.Now()
	}

	window.NewestWalTime = newestWalTime
	window.WindowSeconds = newestWalTime.Sub(oldestBackup).Seconds()
	window.IsValid = true

	return window
}
