package postgres

import "time"

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	BackupName       string    `json:"backup_name"`
	CreationTime     time.Time `json:"creation_time"`
	ModificationTime time.Time `json:"modification_time"`
	WalFileName      string    `json:"wal_file_name"`
}
