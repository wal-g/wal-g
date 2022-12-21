package internal

import "time"

// BackupTimeWithMetadata is used to sort backups by
// latest modified time or creation time.
type BackupTimeWithMetadata struct {
	BackupTime

	StartTime time.Time
}
