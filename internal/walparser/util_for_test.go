package walparser

import (
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func AssertReaderIsEmpty(t *testing.T, reader io.Reader) {
	buf := make([]byte, 1)
	_, err := reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}
