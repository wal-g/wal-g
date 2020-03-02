package internal_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/walparser"
)

func TestSaveLoadDeltaFile(t *testing.T) {
	deltaFile := &internal.DeltaFile{
		Locations: []walparser.BlockLocation{
			*walparser.NewBlockLocation(1, 2, 3, 4),
			*walparser.NewBlockLocation(5, 6, 7, 8),
		},
		WalParser: walparser.NewWalParser(),
	}

	var deltaFileData bytes.Buffer
	err := deltaFile.Save(&deltaFileData)
	assert.NoError(t, err)

	loadedDeltaFile, err := internal.LoadDeltaFile(&deltaFileData)
	assert.NoError(t, err)

	assert.Equal(t, deltaFile, loadedDeltaFile)
}
