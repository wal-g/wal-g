package internal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

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
