package internal

import (
	"time"

	"github.com/wal-g/wal-g/internal/printlist"
)

// BackupTime is used to sort backups by latest modified time.
type BackupTime struct {
	BackupName  string    `json:"backup_name"`
	Time        time.Time `json:"time"`
	WalFileName string    `json:"wal_file_name"`
	StorageName string    `json:"storage_name"`
}

func (bt BackupTime) PrintableFields() []printlist.TableField {
	prettyTime := PrettyFormatTime(bt.Time)
	return []printlist.TableField{
		{
			Name:       "backup_name",
			PrettyName: "Backup name",
			Value:      bt.BackupName,
		},
		{
			Name:        "modified",
			PrettyName:  "Modified",
			Value:       FormatTime(bt.Time),
			PrettyValue: &prettyTime,
		},
		{
			Name:       "wal_file_name",
			PrettyName: "WAL file name",
			Value:      bt.WalFileName,
		},
		{
			Name:       "storage_name",
			PrettyName: "Storage name",
			Value:      bt.StorageName,
		},
	}
}
