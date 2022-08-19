package internal

import (
	"io"
	"sync/atomic"
)

func NewWithSizeReader(underlying io.Reader, readSize *int64) *WithSizeReader {
	return &WithSizeReader{underlying: underlying, readSize: readSize}
}

type WithSizeReader struct {
	underlying io.Reader
	readSize   *int64
}

func (reader *WithSizeReader) Read(p []byte) (n int, err error) {
	n, err = reader.underlying.Read(p)
	atomic.AddInt64(reader.readSize, int64(n))
	return
}
