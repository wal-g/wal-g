package testtools

import (
	"io"
)

type MockCompressor struct{}

func (compressor *MockCompressor) NewWriter(writer io.Writer) io.WriteCloser {
	return &NopCloserWriter{
		writer,
	}
}

func (compressor *MockCompressor) FileExtension() string {
	return "mock"
}
