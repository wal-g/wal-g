package postgres_test

import (
	"bytes"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/testtools"
)

func TestIsComplete_Complete(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	partFile.PreviousWalHead = make([]byte, 0)
	for i := range partFile.WalHeads {
		partFile.WalHeads[i] = make([]byte, 0)
	}
	for i := range partFile.WalTails {
		partFile.WalTails[i] = make([]byte, 0)
	}
	assert.True(t, partFile.IsComplete())
}

func TestIsComplete_NotComplete(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	partFile.PreviousWalHead = make([]byte, 0)
	for i := range partFile.WalHeads {
		partFile.WalHeads[i] = make([]byte, 0)
	}
	for i := range partFile.WalTails {
		partFile.WalTails[i] = make([]byte, 0)
	}

	partFile.PreviousWalHead = nil
	assert.False(t, partFile.IsComplete())
	partFile.PreviousWalHead = make([]byte, 0)

	partFile.WalTails[4] = nil
	assert.False(t, partFile.IsComplete())
	partFile.WalTails[4] = make([]byte, 0)

	partFile.WalHeads[6] = nil
	assert.False(t, partFile.IsComplete())
	partFile.WalHeads[6] = make([]byte, 0)
}

func TestSaveLoadWalPartFile(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	partFile.PreviousWalHead = []byte{1, 2, 3, 4, 5}
	partFile.WalHeads[5] = []byte{6, 7, 7, 8, 9}
	partFile.WalTails[10] = []byte{10, 11, 12, 13, 14}

	var partFileData bytes.Buffer
	err := partFile.Save(&partFileData)
	assert.NoError(t, err)

	loadedPartFile, err := postgres.LoadPartFile(&partFileData)
	assert.NoError(t, err)

	assert.Equal(t, *partFile, *loadedPartFile)
}

func TestCombineRecords(t *testing.T) {
	partFile := postgres.NewWalPartFile()
	xLogRecord, recordData := testtools.GetXLogRecordData()
	partFile.WalHeads[1] = recordData[:16]
	partFile.WalTails[2] = recordData[16:]

	actualRecords, err := partFile.CombineRecords()
	assert.NoError(t, err)
	assert.Equal(t, []walparser.XLogRecord{xLogRecord}, actualRecords)
}

func TestIsPartiallyFilledPartFile(t *testing.T) {
	t.Run("not partially filled part file when PreviousWalHead is not nil", func(t *testing.T) {
		partFile := postgres.NewWalPartFile()
		// Set PreviousWalHead to indicate this is not the partially filled part file
		partFile.PreviousWalHead = []byte{1, 2, 3}

		isPartiallyFilled, index, err := partFile.IsPartiallyFilledPartFile()
		assert.NoError(t, err)
		assert.False(t, isPartiallyFilled)
		assert.Equal(t, -1, index)
	})

	t.Run("partially filled part file with valid index", func(t *testing.T) {
		partFile := postgres.NewWalPartFile()
		// Set both WalHead and WalTail at index 1
		partFile.WalHeads[1] = []byte{1, 2, 3}
		partFile.WalTails[1] = []byte{4, 5, 6}

		isPartiallyFilled, index, err := partFile.IsPartiallyFilledPartFile()
		assert.NoError(t, err)
		assert.True(t, isPartiallyFilled)
		assert.Equal(t, 1, index)
	})

	t.Run("inconsistent state between WalHead and WalTail", func(t *testing.T) {
		partFile := postgres.NewWalPartFile()
		// Create inconsistent state by setting only WalHead
		partFile.WalHeads[0] = []byte{1, 2, 3}

		isPartiallyFilled, index, err := partFile.IsPartiallyFilledPartFile()
		assert.Error(t, err)
		assert.False(t, isPartiallyFilled)
		assert.Equal(t, -1, index)
		assert.Contains(t, err.Error(), "inconsistent state between wal heads and wal tails")
	})

	t.Run("all nil elements", func(t *testing.T) {
		partFile := postgres.NewWalPartFile()
		// Test with a newly created part file where all elements are nil

		isPartiallyFilled, index, err := partFile.IsPartiallyFilledPartFile()
		assert.NoError(t, err)
		assert.False(t, isPartiallyFilled)
		assert.Equal(t, -1, index)
	})
}
