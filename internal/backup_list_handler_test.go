package internal_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"testing"
	"time"
)

type someError struct {
	error
}

func TestHandleBackupListWriteBackups(t *testing.T) {
	backups := []internal.BackupTime {
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
	backups := []internal.BackupTime {
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

