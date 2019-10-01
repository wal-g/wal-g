package internal

import (
	"io"
	"sync/atomic"
)

type WithSizeReader struct {
	underlying io.Reader
	tarSize    *int64
}

func (reader *WithSizeReader) Read(p []byte) (n int, err error) {
	n, err = reader.underlying.Read(p)
	atomic.AddInt64(reader.tarSize, int64(n))
	return
}
