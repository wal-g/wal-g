package multistorage

import (
	"io"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/multistorage/stats"
)

var _ io.Reader = &countReader{}

// countReader wraps an io.Reader and counts how many bytes were read from it.
type countReader struct {
	io.Reader
	statsCollector stats.Collector
	readBytes      atomic.Int64
}

func newCountReader(reader io.Reader) *countReader {
	return &countReader{
		Reader: reader,
	}
}

func (r *countReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.readBytes.Add(int64(n))
	return n, err
}

func (r *countReader) ReadBytes() int64 {
	return r.readBytes.Load()
}
