package internal

import "time"

// BackupTime is used to sort backups by latest modified time.
type BackupTime struct {
	BackupName  string    `json:"backup_name"`
	Time        time.Time `json:"time"`
	WalFileName string    `json:"wal_file_name"`
	StorageName string    `json:"storage_name"`
}
