package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestBackupDetail_PrintableFields(t *testing.T) {
	bd := BackupDetail{
		BackupName:       "my first backup",
		ModifyTime:       time.Unix(1692800000, 0).UTC(),
		BinLogStart:      "start",
		BinLogEnd:        "end",
		StartLocalTime:   time.Unix(1692811111, 0).UTC(),
		StopLocalTime:    time.Unix(1692822222, 0).UTC(),
		UncompressedSize: 200000,
		CompressedSize:   100000,
		Hostname:         "my-favourite-host",
		IsPermanent:      true,
	}
	got := bd.PrintableFields()
	prettyModifiedTime := "Wednesday, 23-Aug-23 14:13:20 UTC"
	prettyStartTime := "Wednesday, 23-Aug-23 17:18:31 UTC"
	prettyFinishTime := "Wednesday, 23-Aug-23 20:23:42 UTC"
	want := []printlist.TableField{
		{
			Name:        "name",
			PrettyName:  "Name",
			Value:       "my first backup",
			PrettyValue: nil,
		},
		{
			Name:        "last_modified",
			PrettyName:  "Last modified",
			Value:       "2023-08-23T14:13:20Z",
			PrettyValue: &prettyModifiedTime,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       "2023-08-23T17:18:31Z",
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "stop_time",
			PrettyName:  "Stop time",
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
			Name:        "binlog_start",
			PrettyName:  "Binlog start",
			Value:       "start",
			PrettyValue: nil,
		},
		{
			Name:        "binlog_end",
			PrettyName:  "Binlog end",
			Value:       "end",
			PrettyValue: nil,
		},
		{
			Name:        "uncompressed_size",
			PrettyName:  "Uncompressed size",
			Value:       "200000",
			PrettyValue: nil,
		},
		{
			Name:        "compressed_size",
			PrettyName:  "Compressed size",
			Value:       "100000",
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
