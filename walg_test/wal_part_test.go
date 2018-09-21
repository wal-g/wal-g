package walg_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"testing"
)

func TestSaveLoadWalPart(t *testing.T) {
	walPart := walg.NewWalPart(walg.WalTailType, 5, []byte{1, 2, 3, 4, 5})

	var walPartData bytes.Buffer
	err := walPart.Save(&walPartData)
	assert.NoError(t, err)

	loadedWalPart, err := walg.LoadWalPart(&walPartData)
	assert.NoError(t, err)

	assert.Equal(t, walPart, loadedWalPart)
}
