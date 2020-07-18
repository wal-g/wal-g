package limited

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
}

func NewReader(reader io.Reader, limiter *rate.Limiter) *Reader {
	return &Reader{reader, limiter}
}

func (r *Reader) Read(buf []byte) (int, error) {
	end := len(buf)
	if r.limiter.Burst() < end {
		end = r.limiter.Burst()
	}
	n, err := r.reader.Read(buf[:end])

	if err != nil {
		limiterErr := r.limiter.WaitN(context.TODO(), utility.Max(n, 0))
		if limiterErr != nil {
			tracelog.ErrorLogger.Printf("Error happened while limiting: %+v\n", limiterErr)
		}
		return n, err
	}

	err = r.limiter.WaitN(context.TODO(), n)
	return n, err
}
