package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestSavePreviousWalTail(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := walg.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	previousWalTail := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SavePreviousWalTail(previousWalTail)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := walg.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, previousWalTail, partFile.WalTails[walg.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_MiddleWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := walg.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := walg.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[walg.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_LastWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := walg.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := walg.NewWalPartRecorder(LastWalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := walg.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[walg.GetPositionInDelta(LastWalFilename)])

	nextWalFilename, err := walg.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := walg.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	nextPartFile, err := manager.GetPartFile(nextDeltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, nextPartFile.PreviousWalHead)
}
