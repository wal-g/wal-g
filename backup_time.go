package walg

import "time"

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	Name        string
	Time        time.Time
	WalFileName string
}
