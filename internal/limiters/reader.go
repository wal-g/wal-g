package limiters

import (
	"context"
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/time/rate"
)

type Reader struct {
	reader  io.Reader
	limiter *rate.Limiter
	ctx     context.Context
}

func NewReader(ctx context.Context, reader io.Reader, limiter *rate.Limiter) *Reader {
	return &Reader{
		reader:  reader,
		limiter: limiter,
		ctx:     ctx,
	}
}

func (r *Reader) Read(buf []byte) (int, error) {
	end := len(buf)
	if r.limiter.Burst() < end {
		end = r.limiter.Burst()
	}
	n, err := r.reader.Read(buf[:end])

	if err != nil {
		limiterErr := r.limiter.WaitN(r.ctx, utility.Max(n, 0))
		if limiterErr != nil {
			tracelog.ErrorLogger.Printf("Error happened while limiting: %+v\n", limiterErr)
		}
		return n, err
	}

	err = r.limiter.WaitN(r.ctx, n)
	return n, err
}
