package utility

import (
	"io"
	"sync/atomic"
)

func NewWithSizeWriter(underlying io.Writer, writeSize *int64) *WithSizeWriter {
	return &WithSizeWriter{underlying: underlying, writeSize: writeSize}
}

func NewWithSizeWriteCloser(underlying io.WriteCloser, writeSize *int64) *WithSizeWriteCloser {
	return &WithSizeWriteCloser{
		WithSizeWriter: WithSizeWriter{
			underlying: underlying,
			writeSize:  writeSize,
		},
		writeCloser: underlying,
	}
}

type WithSizeWriter struct {
	underlying io.Writer
	writeSize  *int64
}

type WithSizeWriteCloser struct {
	WithSizeWriter
	writeCloser io.WriteCloser
}

func (reader *WithSizeWriter) Write(p []byte) (n int, err error) {
	n, err = reader.underlying.Write(p)
	atomic.AddInt64(reader.writeSize, int64(n))
	return
}

func (w *WithSizeWriteCloser) Close() error {
	return w.writeCloser.Close()
}
