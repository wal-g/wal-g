package test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
)

func TestSaveLoadWalPart(t *testing.T) {
	walPart := internal.NewWalPart(internal.WalTailType, 5, []byte{1, 2, 3, 4, 5})

	var walPartData bytes.Buffer
	err := walPart.Save(&walPartData)
	assert.NoError(t, err)

	loadedWalPart, err := internal.LoadWalPart(&walPartData)
	assert.NoError(t, err)

	assert.Equal(t, walPart, loadedWalPart)
}
