package postgres_test

import (
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/testtools"
)

func TestSavePreviousWalTail(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := postgres.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	previousWalTail := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SavePreviousWalTail(previousWalTail)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := postgres.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, previousWalTail, partFile.WalTails[postgres.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_MiddleWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := postgres.NewWalPartRecorder(WalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := postgres.GetDeltaFilenameFor(WalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[postgres.GetPositionInDelta(WalFilename)])
}

func TestSaveNextWalHead_LastWalFile(t *testing.T) {
	dataFolder := testtools.NewMockDataFolder()
	manager := postgres.NewDeltaFileManager(dataFolder)

	walPartRecorder, err := postgres.NewWalPartRecorder(LastWalFilename, manager)
	assert.NoError(t, err)
	nextWalHead := []byte{1, 2, 3, 4, 5}
	err = walPartRecorder.SaveNextWalHead(nextWalHead)
	assert.NoError(t, err)
	manager.FlushFiles(nil)

	deltaFilename, err := postgres.GetDeltaFilenameFor(LastWalFilename)
	assert.NoError(t, err)
	partFile, err := manager.GetPartFile(deltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, partFile.WalHeads[postgres.GetPositionInDelta(LastWalFilename)])

	nextWalFilename, err := postgres.GetNextWalFilename(LastWalFilename)
	assert.NoError(t, err)
	nextDeltaFilename, err := postgres.GetDeltaFilenameFor(nextWalFilename)
	assert.NoError(t, err)
	nextPartFile, err := manager.GetPartFile(nextDeltaFilename)
	assert.NoError(t, err)

	assert.Equal(t, nextWalHead, nextPartFile.PreviousWalHead)
}
