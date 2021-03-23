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

func TestHandleBackupListWriteBackups(t *testing.T) {
	backups := []internal.BackupTime{
		{
			BackupName:       "backup000",
			ModificationTime: time.Time{},
			WalFileName:      "wallName0",
		},
		{
			BackupName:       "backup001",
			ModificationTime: time.Time{},
			WalFileName:      "wallName1",
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
			BackupName:       "backup000",
			ModificationTime: time.Time{},
			WalFileName:      "wallName0",
		},
		{
			BackupName:       "backup001",
			ModificationTime: time.Time{},
			WalFileName:      "wallName1",
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
	expectedRes :=
`+---+-----------+---------+----------+-----------------------------------+
| # | NAME      | CREATED | MODIFIED | WAL SEGMENT BACKUP START          |
+---+-----------+---------+----------+-----------------------------------+
| 0 | backup000 | -       | -        | veryVeryVeryVeryVeryLongWallName0 |
| 1 | backup001 | -       | -        | veryVeryVeryVeryVeryLongWallName1 |
+---+-----------+---------+----------+-----------------------------------+
`
	backups := []internal.BackupTime{
		{
			BackupName:       "backup000",
			ModificationTime: time.Time{},
			WalFileName:      "veryVeryVeryVeryVeryLongWallName0",
		},
		{
			BackupName:       "backup001",
			ModificationTime: time.Time{},
			WalFileName:      "veryVeryVeryVeryVeryLongWallName1",
		},
	}

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_ShortColumnsValues(t *testing.T) {
	expectedRes :=
`+---+------+---------+----------+--------------------------+
| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |
+---+------+---------+----------+--------------------------+
| 0 | b0   | -       | -        | shortWallName0           |
| 1 | b1   | -       | -        | shortWallName1           |
+---+------+---------+----------+--------------------------+
`
	backups := []internal.BackupTime{
		{
			BackupName:       "b0",
			ModificationTime: time.Time{},
			WalFileName:      "shortWallName0",
		},
		{
			BackupName:       "b1",
			ModificationTime: time.Time{},
			WalFileName:      "shortWallName1",
		},
	}

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_WriteNoBackupList(t *testing.T) {
	expectedRes :=
`+---+------+---------+----------+--------------------------+
| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |
+---+------+---------+----------+--------------------------+
+---+------+---------+----------+--------------------------+
`
	backups := make([]internal.BackupTime, 0)

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyBackupList_EmptyColumnsValues(t *testing.T) {
	expectedRes :=
`+---+------+---------+----------+--------------------------+
| # | NAME | CREATED | MODIFIED | WAL SEGMENT BACKUP START |
+---+------+---------+----------+--------------------------+
| 0 |      | -       | -        | shortWallName0           |
| 1 | b1   | -       | -        |                          |
| 2 |      | -       | -        |                          |
+---+------+---------+----------+--------------------------+
`
	backups := []internal.BackupTime{
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

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(backups, &b)

	assert.Equal(t, expectedRes, b.String())
}
