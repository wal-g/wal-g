package walparser

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"testing"
	"github.com/pkg/errors"
)

func TestWalPageReader_ReadPageData(t *testing.T) {
	initialData := make([]byte, WalPageSize)
	for i := 0; i < int(WalPageSize); i++ {
		initialData[i] = byte(rand.Int())
	}
	reader := bytes.NewReader(initialData)
	pageReader := WalPageReader{reader}
	readData, err := pageReader.ReadPageData()
	assert.NoError(t, err)
	assert.Equal(t, initialData, readData)
}

func TestWalPageReader_SmallPageError(t *testing.T) {
	initialData := make([]byte, 1000)
	reader := bytes.NewReader(initialData)
	pageReader := WalPageReader{reader}
	_, err := pageReader.ReadPageData()
	assert.Equal(t, io.ErrUnexpectedEOF, errors.Cause(err))
}

func TestWalPageReader_EOF(t *testing.T) {
	reader := bytes.NewReader(nil)
	pageReader := WalPageReader{reader}
	_, err := pageReader.ReadPageData()
	assert.Equal(t, io.EOF, err)
}
