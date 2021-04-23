package postgres_test

import (
	"testing"
	"bytes"
	"time"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/stretchr/testify/assert"
)

type someError struct {
	error
}

var shortBackups = []postgres.BackupTime{
	{
		BackupName:  "b0",
		ModificationTime:        time.Time{},
		WalFileName: "shortWallName0",
	},
	{
		BackupName:  "b1",
		ModificationTime:        time.Time{},
		WalFileName: "shortWallName1",
	},
}

var longBackups = []postgres.BackupTime{
	{
		BackupName:  "backup000",
		ModificationTime:        time.Time{},
		WalFileName: "veryVeryVeryVeryVeryLongWallName0",
	},
	{
		BackupName:  "backup001",
		ModificationTime:        time.Time{},
		WalFileName: "veryVeryVeryVeryVeryLongWallName1",
	},
}

var emptyColonsBackups = []postgres.BackupTime{
	{
		ModificationTime:        time.Time{},
		WalFileName: "shortWallName0",
	},
	{
		BackupName: "b1",
		ModificationTime:       time.Time{},
	},
	{
		ModificationTime: time.Time{},
	},
}

func TestWritePrettyBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "+---+-----------+---------+----------+-----------------------------------+\n" +
                   "| # | NAME      | CREATED | MODIFIED | WAL SEGMENT BACKUP START          |\n" +
                   "+---+-----------+---------+----------+-----------------------------------+\n" +
                   "| 0 | backup000 | -       | -        | veryVeryVeryVeryVeryLongWallName0 |\n" +
                   "| 1 | backup001 | -       | -        | veryVeryVeryVeryVeryLongWallName1 |\n" +
                   "+---+-----------+---------+----------+-----------------------------------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupList(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "+---+------+---------+----------+--------------------------+\n" +
                   "| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |\n" +
                   "+---+------+---------+----------+--------------------------+\n" +
                   "| 0 | b0   | -       | -        | shortWallName0           |\n" +
                   "| 1 | b1   | -       | -        | shortWallName1           |\n" +
                   "+---+------+---------+----------+--------------------------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupList(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_WriteNoBackupList(t *testing.T) {
	expectedRes := "+---+------+---------+----------+--------------------------+\n" +
                   "| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |\n" +
                   "+---+------+---------+----------+--------------------------+\n" +
                   "+---+------+---------+----------+--------------------------+\n"
	backups := make([]postgres.BackupTime, 0)

	b := bytes.Buffer{}
	postgres.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "+---+------+---------+----------+--------------------------+\n" +
                   "| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |\n" +
                   "+---+------+---------+----------+--------------------------+\n" +
                   "| 0 |      | -       | -        | shortWallName0           |\n" +
                   "| 1 | b1   | -       | -        |                          |\n" +
                   "| 2 |      | -       | -        |                          |\n" +
                   "+---+------+---------+----------+--------------------------+\n"
	b := bytes.Buffer{}
	postgres.WritePrettyBackupList(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_NoBackups(t *testing.T) {
	expectedRes := "name created modified wal_segment_backup_start\n"
	backups := make([]postgres.BackupTime, 0)

	b := bytes.Buffer{}
	postgres.WriteBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes :=  "name created modified wal_segment_backup_start\n" +
	                "     -       -        shortWallName0\n" +
	                "b1   -       -        \n" +
	                "     -       -        \n"
	b := bytes.Buffer{}
	postgres.WriteBackupList(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "name created modified wal_segment_backup_start\n" +
	               "b0   -       -        shortWallName0\n" +
	               "b1   -       -        shortWallName1\n"
	b := bytes.Buffer{}
	postgres.WriteBackupList(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "name      created modified wal_segment_backup_start\n" +
	               "backup000 -       -        veryVeryVeryVeryVeryLongWallName0\n" +
	               "backup001 -       -        veryVeryVeryVeryVeryLongWallName1\n"
	b := bytes.Buffer{}
	postgres.WriteBackupList(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}
