package internal_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

type someError struct {
	error
}

var shortBackups = []internal.BackupTime{
	{
		BackupName:  "b0",
		Time:        time.Time{},
		WalFileName: "shortWallName0",
	},
	{
		BackupName:  "b1",
		Time:        time.Time{},
		WalFileName: "shortWallName1",
	},
}

var longBackups = []internal.BackupTime{
	{
		BackupName:  "backup000",
		Time:        time.Time{},
		WalFileName: "veryVeryVeryVeryVeryLongWallName0",
	},
	{
		BackupName:  "backup001",
		Time:        time.Time{},
		WalFileName: "veryVeryVeryVeryVeryLongWallName1",
	},
}

var emptyColonsBackups = []internal.BackupTime{
	{
		Time:        time.Time{},
		WalFileName: "shortWallName0",
	},
	{
		BackupName: "b1",
		Time:       time.Time{},
	},
	{
		Time: time.Time{},
	},
}

func TestHandleBackupListWriteBackups(t *testing.T) {
	backups := []internal.BackupTime{
		{
			BackupName:  "backup000",
			Time:        time.Time{},
			WalFileName: "wallName0",
		},
		{
			BackupName:  "backup001",
			Time:        time.Time{},
			WalFileName: "wallName1",
		},
	}

	getBackupsFunc := func() ([]internal.BackupTime, error) {
		return backups, nil
	}
	writeBackupListCallsCount := 0
	var writeBackupListCallArgs []internal.BackupTime
	writeBackupListFunc := func(backups []internal.BackupTime) {
		writeBackupListCallsCount++
		writeBackupListCallArgs = backups
	}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getBackupsFunc,
		writeBackupListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, writeBackupListCallsCount)
	assert.Equal(t, backups, writeBackupListCallArgs)
}

func TestHandleBackupListLogError(t *testing.T) {
	backups := []internal.BackupTime{
		{
			BackupName:  "backup000",
			Time:        time.Time{},
			WalFileName: "wallName0",
		},
		{
			BackupName:  "backup001",
			Time:        time.Time{},
			WalFileName: "wallName1",
		},
	}
	someErrorInstance := someError{errors.New("some error")}
	getBackupsFunc := func() ([]internal.BackupTime, error) {
		return backups, someErrorInstance
	}
	writeBackupListFunc := func(backups []internal.BackupTime) {}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getBackupsFunc,
		writeBackupListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, errorLogger.Stats.FatalOnErrorCallsCount)
	assert.Equal(t, someErrorInstance, errorLogger.Stats.Err)
}

func TestHandleBackupListLogNoBackups(t *testing.T) {
	getBackupsFunc := func() ([]internal.BackupTime, error) {
		return []internal.BackupTime{}, nil
	}
	writeBackupListFunc := func(backups []internal.BackupTime) {}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getBackupsFunc,
		writeBackupListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, infoLogger.Stats.PrintLnCallsCount)
	assert.Equal(t, "No backups found", infoLogger.Stats.PrintMsg)
	assert.Equal(t, 0, errorLogger.Stats.FatalOnErrorCallsCount)
}

func TestWritePrettyBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "+---+-----------+---------------+-----------------------------------+\n" +
				   "| # | NAME      | LAST MODIFIED | WAL SEGMENT BACKUP START          |\n" +
				   "+---+-----------+---------------+-----------------------------------+\n" +
				   "| 0 | backup000 | -             | veryVeryVeryVeryVeryLongWallName0 |\n" +
				   "| 1 | backup001 | -             | veryVeryVeryVeryVeryLongWallName1 |\n" +
				   "+---+-----------+---------------+-----------------------------------+\n"
	b := bytes.Buffer{}
	internal.WritePrettyBackupList(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "+---+------+---------------+--------------------------+\n" +
				   "| # | NAME | LAST MODIFIED | WAL SEGMENT BACKUP START |\n" +
				   "+---+------+---------------+--------------------------+\n" +
				   "| 0 | b0   | -             | shortWallName0           |\n" +
				   "| 1 | b1   | -             | shortWallName1           |\n" +
				   "+---+------+---------------+--------------------------+\n"
	b := bytes.Buffer{}
	internal.WritePrettyBackupList(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_WriteNoBackupList(t *testing.T) {
	expectedRes := "+---+------+---------------+--------------------------+\n" +
				   "| # | NAME | LAST MODIFIED | WAL SEGMENT BACKUP START |\n" +
				   "+---+------+---------------+--------------------------+\n" +
				   "+---+------+---------------+--------------------------+\n"
	backups := make([]internal.BackupTime, 0)

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "+---+------+---------------+--------------------------+\n" +
				   "| # | NAME | LAST MODIFIED | WAL SEGMENT BACKUP START |\n" +
				   "+---+------+---------------+--------------------------+\n" +
				   "| 0 |      | -             | shortWallName0           |\n" +
				   "| 1 | b1   | -             |                          |\n" +
				   "| 2 |      | -             |                          |\n" +
				   "+---+------+---------------+--------------------------+\n"
	b := bytes.Buffer{}
	internal.WritePrettyBackupList(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_NoBackups(t *testing.T) {
	expectedRes := "name last_modified wal_segment_backup_start\n"
	backups := make([]internal.BackupTime, 0)

	b := bytes.Buffer{}
	internal.WriteBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "name last_modified wal_segment_backup_start\n" +
				   "     -             \n" +
				   "b1   -             \n" +
				   "     -             shortWallName0\n"
	b := bytes.Buffer{}
	internal.WriteBackupList(emptyColonsBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes := "name last_modified wal_segment_backup_start\n" +
				   "b1   -             shortWallName1\n" +
				   "b0   -             shortWallName0\n"
	b := bytes.Buffer{}
	internal.WriteBackupList(shortBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteBackupList_LongColumnsValues(t *testing.T) {
	expectedRes := "name      last_modified wal_segment_backup_start\n" +
                   "backup001 -             veryVeryVeryVeryVeryLongWallName1\n" +
                   "backup000 -             veryVeryVeryVeryVeryLongWallName0\n"
	b := bytes.Buffer{}
	internal.WriteBackupList(longBackups, &b)

	assert.Equal(t, expectedRes, b.String())
}
