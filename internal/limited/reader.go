package limited

import (
	"context"
	"golang.org/x/time/rate"
	"io"
)

type Reader struct {
	reader  io.Reader
	limiter *rate.Limiter
}

func NewReader(reader io.Reader, limiter *rate.Limiter) *Reader {
	return &Reader{reader, limiter}
}

func (r *Reader) Read(buf []byte) (int, error) {
	n, err := r.reader.Read(buf)
	if n <= 0 {
		return n, err
	}

	err = r.limiter.WaitN(context.TODO(), n)
	return n, err
}
