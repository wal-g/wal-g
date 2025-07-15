package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupDetail_PrintableFields(t *testing.T) {
	bd := &BackupDetail{
		BackupTime: internal.BackupTime{
			BackupName:  "my first backup",
			Time:        time.Unix(1692800000, 0).UTC(),
			WalFileName: "my/wal/file/name",
			StorageName: "my failover ssh storage",
		},
		ExtendedMetadataDto: ExtendedMetadataDto{
			StartTime:      time.Unix(1692811111, 0).UTC(),
			FinishTime:     time.Unix(1692822222, 0).UTC(),
			DatetimeFormat: "",
			Hostname:       "my-favourite-host",
			DataDir:        "my/personal/files",
			PgVersion:      15,
			StartLsn:       1111111111111111,
			FinishLsn:      2222222222222222,
			IsPermanent:    true,
		},
	}
	got := bd.PrintableFields()
	prettyModifiedTime := "Wednesday, 23-Aug-23 14:13:20 UTC"
	prettyStartTime := "Wednesday, 23-Aug-23 17:18:31 UTC"
	prettyFinishTime := "Wednesday, 23-Aug-23 20:23:42 UTC"
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
			Value:       "2023-08-23T14:13:20Z",
			PrettyValue: &prettyModifiedTime,
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
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       "2023-08-23T17:18:31Z",
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       "2023-08-23T20:23:42Z",
			PrettyValue: &prettyFinishTime,
		},
		{
			Name:        "hostname",
			PrettyName:  "Hostname",
			Value:       "my-favourite-host",
			PrettyValue: nil,
		},
		{
			Name:        "data_dir",
			PrettyName:  "Datadir",
			Value:       "my/personal/files",
			PrettyValue: nil,
		},
		{
			Name:        "pg_version",
			PrettyName:  "PG version",
			Value:       "15",
			PrettyValue: nil,
		},
		{
			Name:        "start_lsn",
			PrettyName:  "Start LSN",
			Value:       "3F28C/B71571C7",
			PrettyValue: nil,
		},
		{
			Name:        "finish_lsn",
			PrettyName:  "Finish LSN",
			Value:       "7E519/6E2AE38E",
			PrettyValue: nil,
		},
		{
			Name:        "is_permanent",
			PrettyName:  "Permanent",
			Value:       "true",
			PrettyValue: nil,
		},
	}
	assert.Equal(t, want, got)
}
