package testtools

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

type MockCompressor struct{}

func (compressor *MockCompressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	return &NopCloserWriter{
		writer,
	}
}

func (compressor *MockCompressor) FileExtension() string {
	return "mock"
}
