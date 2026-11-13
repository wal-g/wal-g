package archive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/fileparts"
	"github.com/wal-g/wal-g/internal/printlist"
)

func TestRedisMetaConstructorPreservesFinalizedSentinel(t *testing.T) {
	constructor := &RedisMetaConstructor{}
	backup := constructor.MetaInfo().(*Backup)
	backup.BackupName = "aof_20261109T000000Z"
	backup.DataSize = 8192
	backup.BackupSize = 4096
	backup.FileParts = &fileparts.Info{PartSize: 1024, Files: map[string]int64{"/appendonly.aof.1.base.rdb": 8192}}

	require.NoError(t, constructor.Finalize(t.Context(), backup.BackupName))
	finalized := constructor.MetaInfo().(*Backup)
	require.Same(t, backup, finalized)
	require.False(t, finalized.FinishLocalTime.IsZero())
	require.Equal(t, int64(8192), finalized.DataSize)
	require.Equal(t, int64(4096), finalized.BackupSize)
	require.Equal(t, backup.FileParts, finalized.FileParts)
}

func TestBackup_PrintableFields(t *testing.T) {
	b := Backup{
		BackupName:      "my first backup",
		StartLocalTime:  time.Unix(1692811111, 0).UTC(),
		FinishLocalTime: time.Unix(1692822222, 0).UTC(),
		DataSize:        100000,
		BackupSize:      200000,
		Permanent:       true,
		UserData:        []string{"a", "b", "c"},
		Version:         "4.5.4",
		BackupType:      "rdb",
		HasTS:           true,
		TSBackupID:      "ts-id",
		TSBackupPath:    "/var/lib/redis/ext/ts-id",
		TSDataSize:      300000,
		TSFileCount:     17,
	}
	got := b.PrintableFields()
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
			Name:        "user_data",
			PrettyName:  "UserData",
			Value:       "[\"a\",\"b\",\"c\"]",
			PrettyValue: nil,
		},
		{
			Name:        "data_size",
			PrettyName:  "Data size",
			Value:       "100000",
			PrettyValue: nil,
		},
		{
			Name:        "backup_size",
			PrettyName:  "Backup size",
			Value:       "200000",
			PrettyValue: nil,
		},
		{
			Name:        "permanent",
			PrettyName:  "Permanent",
			Value:       "true",
			PrettyValue: nil,
		},
		{
			Name:        "backup_type",
			PrettyName:  "Backup type",
			Value:       "rdb",
			PrettyValue: nil,
		},
		{
			Name:        "version",
			PrettyName:  "Backup version",
			Value:       "4.5.4",
			PrettyValue: nil,
		},
		{
			Name:        "used_memory",
			PrettyName:  "Used memory (limited by maxmemory)",
			Value:       "0",
			PrettyValue: nil,
		},
		{
			Name:        "used_memory_rss",
			PrettyName:  "Used memory (as seen by OS))",
			Value:       "0",
			PrettyValue: nil,
		},
		{
			Name:        "has_ts",
			PrettyName:  "Has TS",
			Value:       "true",
			PrettyValue: nil,
		},
		{
			Name:        "ts_backup_id",
			PrettyName:  "TS backup ID",
			Value:       "ts-id",
			PrettyValue: nil,
		},
		{
			Name:        "ts_backup_path",
			PrettyName:  "TS backup path",
			Value:       "/var/lib/redis/ext/ts-id",
			PrettyValue: nil,
		},
		{
			Name:        "ts_data_size",
			PrettyName:  "TS data size",
			Value:       "300000",
			PrettyValue: nil,
		},
		{
			Name:        "ts_file_count",
			PrettyName:  "TS file count",
			Value:       "17",
			PrettyValue: nil,
		},
	}
	assert.Equal(t, want, got)
}
