package internal

import "time"

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	BackupName  string
	Time        time.Time
	WalFileName string
}
