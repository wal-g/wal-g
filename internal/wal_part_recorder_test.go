package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestSavePreviousWalTail(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := internal.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	previousWalTail := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SavePreviousWalTail(previousWalTail)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := internal.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, previousWalTail, partFile.WalTails[internal.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_MiddleWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := internal.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := internal.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[internal.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_LastWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := internal.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := internal.NewWalPartRecorder(LastWalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := internal.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[internal.GetPositionInDelta(LastWalFilename)])

	nextWalFilename, err := internal.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := internal.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	nextPartFile, err := manager.GetPartFile(nextDeltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, nextPartFile.PreviousWalHead)
}
