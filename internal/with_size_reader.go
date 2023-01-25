package internal

import (
	"io"
	"sync/atomic"
)

func NewWithSizeReader(underlying io.Reader, readSize *int64) *WithSizeReader {
	return &WithSizeReader{underlying: underlying, readSize: readSize}
}

func NewWithSizeReadCloser(underlying io.ReadCloser, readSize *int64) *WithSizeReadCloser {
	return &WithSizeReadCloser{
		WithSizeReader: WithSizeReader{
			underlying: underlying,
			readSize:   readSize,
		},
		readCloser: underlying,
	}
}

type WithSizeReader struct {
	underlying io.Reader
	readSize   *int64
}

type WithSizeReadCloser struct {
	WithSizeReader
	readCloser io.ReadCloser
}

func (reader *WithSizeReader) Read(p []byte) (n int, err error) {
	n, err = reader.underlying.Read(p)
	atomic.AddInt64(reader.readSize, int64(n))
	return
}

func (w *WithSizeReadCloser) Close() error {
	return w.readCloser.Close()
}
