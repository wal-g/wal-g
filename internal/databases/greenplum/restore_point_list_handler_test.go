package greenplum_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

type someError struct {
	error
}

var shortRestorePoints = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			BackupName:  "r0",
			Time:        time.Time{},
			WalFileName: "shortWallName0",
		},
	},
	{
		BackupTime: internal.BackupTime{
			BackupName:  "r1",
			Time:        time.Time{},
			WalFileName: "shortWallName1",
		},
	},
}

var longRestorePoints = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			BackupName:  "restorePoint000",
			Time:        time.Time{},
			WalFileName: "veryVeryVeryVeryVeryLongWallName0",
		},
	},
	{
		BackupTime: internal.BackupTime{
			BackupName:  "restorePoint001",
			Time:        time.Time{},
			WalFileName: "veryVeryVeryVeryVeryLongWallName1",
		},
	},
}

var emptyColonsRestorePoints = []internal.BackupTimeWithMetadata{
	{
		BackupTime: internal.BackupTime{
			Time:        time.Time{},
			WalFileName: "shortWallName0",
		},
	},
	{
		BackupTime: internal.BackupTime{
			BackupName: "r1",
			Time:       time.Time{},
		},
	},
	{
		BackupTime: internal.BackupTime{
			Time: time.Time{},
		},
	},
}

func TestHandleRestorePointListWriteBackups(t *testing.T) {
	restorePoints := []internal.BackupTimeWithMetadata{
		{
			BackupTime: internal.BackupTime{
				BackupName:  "restorePoint000",
				Time:        time.Time{},
				WalFileName: "wallName0",
			},
		},
		{
			BackupTime: internal.BackupTime{
				BackupName:  "restorePoint001",
				Time:        time.Time{},
				WalFileName: "wallName1",
			},
		},
	}

	getRestorePointsFunc := func() ([]internal.BackupTimeWithMetadata, error) {
		return restorePoints, nil
	}
	writeRestorePointListCallsCount := 0
	var writeBackupListCallArgs []internal.BackupTimeWithMetadata
	writeRestorePointListFunc := func(restorePoints []internal.BackupTimeWithMetadata) {
		writeRestorePointListCallsCount++
		writeBackupListCallArgs = restorePoints
	}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getRestorePointsFunc,
		writeRestorePointListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, writeRestorePointListCallsCount)
	assert.Equal(t, restorePoints, writeBackupListCallArgs)
}

func TestHandleRestorePointListLogError(t *testing.T) {
	restorePoints := []internal.BackupTimeWithMetadata{
		{
			BackupTime: internal.BackupTime{
				BackupName:  "restorePoint000",
				Time:        time.Time{},
				WalFileName: "wallName0",
			},
		},
		{
			BackupTime: internal.BackupTime{
				BackupName:  "restorePoint001",
				Time:        time.Time{},
				WalFileName: "wallName1",
			},
		},
	}
	someErrorInstance := someError{errors.New("some error")}
	getRestorePointsFunc := func() ([]internal.BackupTimeWithMetadata, error) {
		return restorePoints, someErrorInstance
	}
	writeRestorePointListFunc := func(restorePoints []internal.BackupTimeWithMetadata) {}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getRestorePointsFunc,
		writeRestorePointListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, errorLogger.Stats.FatalOnErrorCallsCount)
	assert.Equal(t, someErrorInstance, errorLogger.Stats.Err)
}

func TestHandleRestorePointListLogNoBackups(t *testing.T) {
	getRestorePointsFunc := func() ([]internal.BackupTimeWithMetadata, error) {
		return []internal.BackupTimeWithMetadata{}, nil
	}
	writeRestorePointListFunc := func(restorePoints []internal.BackupTimeWithMetadata) {}
	infoLogger, errorLogger := testtools.MockLoggers()

	internal.HandleBackupList(
		getRestorePointsFunc,
		writeRestorePointListFunc,
		internal.Logging{InfoLogger: infoLogger, ErrorLogger: errorLogger},
	)

	assert.Equal(t, 1, infoLogger.Stats.PrintLnCallsCount)
	assert.Equal(t, "No backups found", infoLogger.Stats.PrintMsg)
	assert.Equal(t, 1, errorLogger.Stats.FatalOnErrorCallsCount)
	assert.Equal(t, nil, errorLogger.Stats.Err)
}

func TestWritePrettyRestorePointList_LongColumnsValues(t *testing.T) {
	expectedRes :=
		"+---+-----------------+---------+-----------------------------------+\n" +
			"| # | NAME            | CREATED | WAL SEGMENT BACKUP START          |\n" +
			"+---+-----------------+---------+-----------------------------------+\n" +
			"| 0 | restorePoint000 | -       | veryVeryVeryVeryVeryLongWallName0 |\n" +
			"| 1 | restorePoint001 | -       | veryVeryVeryVeryVeryLongWallName1 |\n" +
			"+---+-----------------+---------+-----------------------------------+\n"

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(longRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyRestorePointList_ShortColumnsValues(t *testing.T) {
	expectedRes :=
		"+---+------+---------+--------------------------+\n" +
			"| # | NAME | CREATED | WAL SEGMENT BACKUP START |\n" +
			"+---+------+---------+--------------------------+\n" +
			"| 0 | r0   | -       | shortWallName0           |\n" +
			"| 1 | r1   | -       | shortWallName1           |\n" +
			"+---+------+---------+--------------------------+\n"

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(shortRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyRestorePointList_WriteNoBackupList(t *testing.T) {
	expectedRes :=
		"+---+------+---------+--------------------------+\n" +
			"| # | NAME | CREATED | WAL SEGMENT BACKUP START |\n" +
			"+---+------+---------+--------------------------+\n" +
			"+---+------+---------+--------------------------+\n"

	restorePoints := make([]internal.BackupTimeWithMetadata, 0)

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(restorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWritePrettyRestorePointList_EmptyColumnsValues(t *testing.T) {
	expectedRes :=
		"+---+------+---------+--------------------------+\n" +
			"| # | NAME | CREATED | WAL SEGMENT BACKUP START |\n" +
			"+---+------+---------+--------------------------+\n" +
			"| 0 |      | -       | shortWallName0           |\n" +
			"| 1 | r1   | -       |                          |\n" +
			"| 2 |      | -       |                          |\n" +
			"+---+------+---------+--------------------------+\n"

	b := bytes.Buffer{}
	internal.WritePrettyBackupList(emptyColonsRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteRestorePointList_NoBackups(t *testing.T) {
	expectedRes := "name created wal_segment_backup_start\n"
	restorePoints := make([]internal.BackupTimeWithMetadata, 0)

	b := bytes.Buffer{}
	internal.WriteBackupList(restorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteRestorePointList_EmptyColumnsValues(t *testing.T) {
	expectedRes := "name created wal_segment_backup_start\n" +
		"     -       shortWallName0\n" +
		"r1   -       \n" +
		"     -       \n"

	b := bytes.Buffer{}
	internal.WriteBackupList(emptyColonsRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteRestorePointList_ShortColumnsValues(t *testing.T) {
	expectedRes := "name created wal_segment_backup_start\n" +
		"r0   -       shortWallName0\n" +
		"r1   -       shortWallName1\n"
	b := bytes.Buffer{}
	internal.WriteBackupList(shortRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}

func TestWriteRestorePointList_LongColumnsValues(t *testing.T) {
	expectedRes := "name            created wal_segment_backup_start\n" +
		"restorePoint000 -       veryVeryVeryVeryVeryLongWallName0\n" +
		"restorePoint001 -       veryVeryVeryVeryVeryLongWallName1\n"
	b := bytes.Buffer{}
	internal.WriteBackupList(longRestorePoints, &b)

	assert.Equal(t, expectedRes, b.String())
}
