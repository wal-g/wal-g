package walg

import (
	"io"
	"time"

	"golang.org/x/time/rate"
)

type LimitedReader struct {
	r       io.ReadCloser
	limiter *rate.Limiter
}

var DiskLimiter *rate.Limiter
var NetworkLimiter *rate.Limiter

// NewNetworkLimitReader returns a reader that is rate limited by network limiter
func NewNetworkLimitReader(r io.ReadCloser) io.ReadCloser {
	if NetworkLimiter == nil {
		return r
	}
	return &LimitedReader{
		r:       r,
		limiter: NetworkLimiter,
	}
}

// NewDiskLimitReader returns a reader that is rate limited by disk limiter
func NewDiskLimitReader(r io.ReadCloser) io.ReadCloser {
	if DiskLimiter == nil {
		return r
	}
	return &LimitedReader{
		r:       r,
		limiter: DiskLimiter,
	}
}

func (r *LimitedReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	if n <= 0 {
		return n, err
	}

	err = r.limiter.WaitN(limitReaderCtx, n)
	return n, err
}

func (r *LimitedReader) Close() error {
	return r.r.Close()
}

type emptyContext int

func (*emptyContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*emptyContext) Done() <-chan struct{} {
	return nil
}

func (*emptyContext) Err() error {
	return nil
}

func (*emptyContext) Value(key interface{}) interface{} {
	return nil
}

func (e *emptyContext) String() string {
	switch e {
	case limitReaderCtx:
		return "LimitedReader.Context"
	}
	return "unknown empty Context"
}

var (
	limitReaderCtx = new(emptyContext)
)
