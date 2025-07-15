package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupTime_PrintableFields(t *testing.T) {
	bt := BackupTime{
		BackupName:  "my first backup",
		Time:        time.Unix(1692883732, 0).UTC(),
		WalFileName: "my/wal/file/name",
		StorageName: "my failover ssh storage",
	}
	got := bt.PrintableFields()
	prettyTime := "Thursday, 24-Aug-23 13:28:52 UTC"
	want := []printlist.TableField{
		{
			Name:        "backup_name",
			PrettyName:  "Backup name",
			Value:       "my first backup",
			PrettyValue: nil,
		},
		{
			Name:        "modified",
			PrettyName:  "Modified",
			Value:       "2023-08-24T13:28:52Z",
			PrettyValue: &prettyTime,
		},
		{
			Name:        "wal_file_name",
			PrettyName:  "WAL file name",
			Value:       "my/wal/file/name",
			PrettyValue: nil,
		},
		{
			Name:        "storage_name",
			PrettyName:  "Storage name",
			Value:       "my failover ssh storage",
			PrettyValue: nil,
		},
	}
	assert.Equal(t, want, got)
}
