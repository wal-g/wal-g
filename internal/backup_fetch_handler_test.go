package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestGetRestoredBackupFilesToUnwrap_SimpleFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: testtools.NewBackupFileListBuilder().WithSimple().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, testtools.SimplePath)
}

func TestGetRestoredBackupFilesToUnwrap_IncrementedFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: testtools.NewBackupFileListBuilder().WithIncremented().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, testtools.IncrementedPath)
}

func TestGetRestoredBackupFilesToUnwrap_SkippedFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: testtools.NewBackupFileListBuilder().WithSkipped().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, testtools.SkippedPath)
}

func TestGetRestoredBackupFilesToUnwrap_UtilityFiles(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: testtools.NewBackupFileListBuilder().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Equal(t, internal.UtilityFilePaths, files)
}

func TestGetRestoredBackupFilesToUnwrap_NoMoreFiles(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: testtools.NewBackupFileListBuilder().WithSimple().WithIncremented().WithSkipped().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	expected := map[string]bool{
		testtools.SimplePath:      true,
		testtools.IncrementedPath: true,
		testtools.SkippedPath:     true,
	}
	for utilityPath := range internal.UtilityFilePaths {
		expected[utilityPath] = true
	}
	assert.Equal(t, expected, files)
}

func TestGetBaseFilesToUnwrap_SimpleFile(t *testing.T) {
	fileStates := testtools.NewBackupFileListBuilder().WithSimple().Build()
	currentToUnwrap := map[string]bool{
		testtools.SimplePath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Empty(t, baseToUnwrap)
}

func TestGetBaseFilesToUnwrap_IncrementedFile(t *testing.T) {
	fileStates := testtools.NewBackupFileListBuilder().WithIncremented().Build()
	currentToUnwrap := map[string]bool{
		testtools.IncrementedPath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Equal(t, currentToUnwrap, baseToUnwrap)
}

func TestGetBaseFilesToUnwrap_SkippedFile(t *testing.T) {
	fileStates := testtools.NewBackupFileListBuilder().WithSkipped().Build()
	currentToUnwrap := map[string]bool{
		testtools.SkippedPath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Equal(t, currentToUnwrap, baseToUnwrap)
}
