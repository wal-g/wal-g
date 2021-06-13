package postgres_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

var shortBackups = []postgres.BackupDetail{
	{
		internal.BackupTime{
			BackupName:  "b0",
			Time:        time.Time{},
			WalFileName: "shortWallName0",
		},
		postgres.ExtendedMetadataDto{},
	},
	{
		internal.BackupTime{
			BackupName:  "b1",
			Time:        time.Time{},
			WalFileName: "shortWallName1",
		},
		postgres.ExtendedMetadataDto{},
	},
}

var longBackups = []postgres.BackupDetail{
	{
		internal.BackupTime{
			BackupName:  "backup000",
			Time:        time.Time{},
			WalFileName: "veryVeryVeryVeryVeryLongWallName0",
		},
		postgres.ExtendedMetadataDto{},
	},
	{
		internal.BackupTime{
			BackupName:  "backup001",
			Time:        time.Time{},
			WalFileName: "veryVeryVeryVeryVeryLongWallName1",
		},
		postgres.ExtendedMetadataDto{},
	},
}

var emptyColonsBackups = []postgres.BackupDetail{
	{
		internal.BackupTime{
			Time:        time.Time{},
			WalFileName: "shortWallName0",
		},
		postgres.ExtendedMetadataDto{},
	},
	{
		internal.BackupTime{
			BackupName: "b1",
			Time:       time.Time{},
		},
		postgres.ExtendedMetadataDto{},
	},
	{
		internal.BackupTime{
			Time: time.Time{},
		},
		postgres.ExtendedMetadataDto{},
	},
}

func TestWritePrettyBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "+---+-----------+----------+-----------------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| # | NAME      | MODIFIED | WAL SEGMENT BACKUP START          | START TIME | FINISH TIME | HOSTNAME | DATADIR | PG VERSION | START LSN | FINISH LSN | PERMANENT |\n" +
		"+---+-----------+----------+-----------------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| 0 | backup000 | -        | veryVeryVeryVeryVeryLongWallName0 | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"| 1 | backup001 | -        | veryVeryVeryVeryVeryLongWallName1 | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"+---+-----------+----------+-----------------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupListDetails(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| # | NAME | MODIFIED | WAL SEGMENT BACKUP START | START TIME | FINISH TIME | HOSTNAME | DATADIR | PG VERSION | START LSN | FINISH LSN | PERMANENT |\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| 0 | b0   | -        | shortWallName0           | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"| 1 | b1   | -        | shortWallName1           | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupListDetails(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_WriteNoBackupList(t *testing.T) {
	expectedRes := "+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| # | NAME | MODIFIED | WAL SEGMENT BACKUP START | START TIME | FINISH TIME | HOSTNAME | DATADIR | PG VERSION | START LSN | FINISH LSN | PERMANENT |\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n"
	backups := make([]postgres.BackupDetail, 0)

	b := bytes.Buffer{}
	postgres.WritePrettyBackupListDetails(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| # | NAME | MODIFIED | WAL SEGMENT BACKUP START | START TIME | FINISH TIME | HOSTNAME | DATADIR | PG VERSION | START LSN | FINISH LSN | PERMANENT |\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n" +
		"| 0 |      | -        | shortWallName0           | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"| 1 | b1   | -        |                          | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"| 2 |      | -        |                          | -          | -           |          |         |          0 |         0 |          0 | false     |\n" +
		"+---+------+----------+--------------------------+------------+-------------+----------+---------+------------+-----------+------------+-----------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupListDetails(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_NoBackups(t *testing.T) {
	expectedRes := "name modified wal_segment_backup_start start_time finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n"
	backups := make([]postgres.BackupDetail, 0)

	b := bytes.Buffer{}
	postgres.WriteBackupListDetails(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "name modified wal_segment_backup_start start_time finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"     -        shortWallName0           -          -                             0          0         0          false\n" +
		"b1   -                                 -          -                             0          0         0          false\n" +
		"     -                                 -          -                             0          0         0          false\n"
	b := bytes.Buffer{}
	postgres.WriteBackupListDetails(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "name modified wal_segment_backup_start start_time finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"b0   -        shortWallName0           -          -                             0          0         0          false\n" +
		"b1   -        shortWallName1           -          -                             0          0         0          false\n"

	b := bytes.Buffer{}
	postgres.WriteBackupListDetails(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "name      modified wal_segment_backup_start          start_time finish_time hostname data_dir pg_version start_lsn finish_lsn is_permanent\n" +
		"backup000 -        veryVeryVeryVeryVeryLongWallName0 -          -                             0          0         0          false\n" +
		"backup001 -        veryVeryVeryVeryVeryLongWallName1 -          -                             0          0         0          false\n"

	b := bytes.Buffer{}
	postgres.WriteBackupListDetails(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}
