package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

func TestGetRestoredBackupFilesToUnwrap_SimpleFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: NewBackupFileListBuilder().WithSimple().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, SimplePath)
}

func TestGetRestoredBackupFilesToUnwrap_IncrementedFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: NewBackupFileListBuilder().WithIncremented().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, IncrementedPath)
}

func TestGetRestoredBackupFilesToUnwrap_SkippedFile(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: NewBackupFileListBuilder().WithSkipped().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Contains(t, files, SkippedPath)
}

func TestGetRestoredBackupFilesToUnwrap_UtilityFiles(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: NewBackupFileListBuilder().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	assert.Equal(t, internal.UtilityFilePaths, files)
}

func TestGetRestoredBackupFilesToUnwrap_NoMoreFiles(t *testing.T) {
	sentinelDto := internal.BackupSentinelDto{
		Files: NewBackupFileListBuilder().WithSimple().WithIncremented().WithSkipped().Build(),
	}

	files := internal.GetRestoredBackupFilesToUnwrap(sentinelDto)
	expected := map[string]bool{
		SimplePath:      true,
		IncrementedPath: true,
		SkippedPath:     true,
	}
	for utilityPath := range internal.UtilityFilePaths {
		expected[utilityPath] = true
	}
	assert.Equal(t, expected, files)
}

func TestGetBaseFilesToUnwrap_SimpleFile(t *testing.T) {
	fileStates := NewBackupFileListBuilder().WithSimple().Build()
	currentToUnwrap := map[string]bool{
		SimplePath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Empty(t, baseToUnwrap)
}

func TestGetBaseFilesToUnwrap_IncrementedFile(t *testing.T) {
	fileStates := NewBackupFileListBuilder().WithIncremented().Build()
	currentToUnwrap := map[string]bool{
		IncrementedPath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Equal(t, currentToUnwrap, baseToUnwrap)
}

func TestGetBaseFilesToUnwrap_SkippedFile(t *testing.T) {
	fileStates := NewBackupFileListBuilder().WithSkipped().Build()
	currentToUnwrap := map[string]bool{
		SkippedPath: true,
	}
	baseToUnwrap, err := internal.GetBaseFilesToUnwrap(fileStates, currentToUnwrap)
	assert.NoError(t, err)
	assert.Equal(t, currentToUnwrap, baseToUnwrap)
}
