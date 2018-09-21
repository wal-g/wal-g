package walg_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/walparser"
	"testing"
)

func TestIsComplete_Complete(t *testing.T) {
	partFile := walg.NewWalPartFile()
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
	partFile := walg.NewWalPartFile()
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
	partFile := walg.NewWalPartFile()
	partFile.PreviousWalHead = []byte{1, 2, 3, 4, 5}
	partFile.WalHeads[5] = []byte{6, 7, 7, 8, 9}
	partFile.WalTails[10] = []byte{10, 11, 12, 13, 14}

	var partFileData bytes.Buffer
	err := partFile.Save(&partFileData)
	assert.NoError(t, err)

	loadedPartFile, err := walg.LoadPartFile(&partFileData)
	assert.NoError(t, err)

	assert.Equal(t, *partFile, *loadedPartFile)
}

func TestCombineRecords(t *testing.T) {
	partFile := walg.NewWalPartFile()
	xLogRecord, recordData := GetXLogRecordData()
	partFile.WalHeads[1] = recordData[:16]
	partFile.WalTails[2] = recordData[16:]

	actualRecords, err := partFile.CombineRecords()
	assert.NoError(t, err)
	assert.Equal(t, []walparser.XLogRecord{xLogRecord}, actualRecords)
}
