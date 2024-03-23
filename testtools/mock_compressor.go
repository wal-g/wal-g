package testtools

import (
	"github.com/wal-g/wal-g/internal/ioextensions"
	"io"
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
