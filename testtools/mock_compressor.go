package testtools

import (
	"github.com/wal-g/wal-g/internal"
	"io"
)

type MockCompressor struct{}

func (compressor *MockCompressor) NewWriter(writer io.Writer) internal.ReaderFromWriteCloser {
	return &MockCompressingWriter{writer}
}

func (compressor *MockCompressor) FileExtension() string {
	return "mock"
}

type MockCompressingWriter struct {
	io.Writer
}

func (writer *MockCompressingWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	return utility.FastCopy(writer.Writer, reader)
}

func (writer *MockCompressingWriter) Close() error { return nil }
