package internal

import (
	"encoding/json"

	"github.com/wal-g/wal-g/internal/printlist"
)

// BackupTimeWithMetadata is used to sort backups by
// latest modified time or creation time.
type BackupTimeWithMetadata struct {
	BackupTime
	GenericMetadata
}

func (b BackupTimeWithMetadata) PrintableFields() []printlist.TableField {
	prettyCreatedTime := PrettyFormatTime(b.StartTime)
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      b.BackupTime.BackupName,
		},
		{
			Name:        "created",
			PrettyName:  "Created",
			Value:       FormatTime(b.GenericMetadata.StartTime),
			PrettyValue: &prettyCreatedTime,
		},
		{
			Name:       "wal_segment_backup_start",
			PrettyName: "WAL segment backup start",
			Value:      b.BackupTime.WalFileName,
		},
	}
}

func (b BackupTimeWithMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.BackupTime)
}
