package testtools

import (
	"github.com/wal-g/wal-g"
	"io"
)

type MockCompressor struct{}

func (compressor *MockCompressor) NewWriter(writer io.Writer) walg.ReaderFromWriteCloser {
	return &MockCompressingWriter{writer}
}

func (compressor *MockCompressor) FileExtension() string {
	return "mock"
}

type MockCompressingWriter struct {
	io.Writer
}

func (writer *MockCompressingWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	return walg.WriteTo(writer.Writer, reader)
}

func (writer *MockCompressingWriter) Close() error { return nil }
