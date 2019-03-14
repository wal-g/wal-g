package walparser

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAlignedReader_EOF(t *testing.T) {
	reader := bytes.NewReader(nil)
	alignedReader := NewAlignedReader(reader, 2)
	AssertReaderIsEmpty(t, alignedReader)
}

func TestAlignedReader_Read(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4})
	alignedReader := NewAlignedReader(reader, 2)
	buf := make([]byte, 3)
	n, err := io.ReadFull(alignedReader, buf)
	assert.NoError(t, err)
	assert.Equal(t, n, alignedReader.alreadyRead)
	assert.Equal(t, buf, []byte{1, 2, 3})
}

func TestAlignedReader_ReadAfterAlignment(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	alignedReader := NewAlignedReader(reader, 3)
	buf := make([]byte, 4)
	alignedReader.Read(buf)
	alignedReader.ReadToAlignment()
	readCount, err := alignedReader.Read(buf)
	if err != io.EOF {
		assert.NoError(t, err)
	}
	assert.Equal(t, readCount, 3)
	assert.Equal(t, buf[:3], []byte{7, 8, 9})
}

func TestAlignedReader_ReadToAlignment(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5})
	alignedReader := NewAlignedReader(reader, 2)
	buf := make([]byte, 3)
	alignedReader.Read(buf)
	err := alignedReader.ReadToAlignment()
	assert.NoError(t, err)
	assert.Equal(t, alignedReader.alreadyRead, 4)
}
