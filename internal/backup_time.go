package internal

import "time"

type BackupTimeDenotation int

const (
	ModificationTime BackupTimeDenotation = iota
	CreationTime
	NoData
)

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	BackupName  string    `json:"backup_name"`
	Time        time.Time `json:"time"`
	WalFileName string    `json:"wal_file_name"`
}

type BackupTimeSlice struct {
	Data []BackupTime                   `json:"data"`
	TimeDenotation BackupTimeDenotation `json:"time_denotation"`
}
