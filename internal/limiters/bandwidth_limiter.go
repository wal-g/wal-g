package limiters

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

var DiskLimiter *rate.Limiter

// NetworkLimiter throttles storage traffic via LimitedFolder, see internal.NewLimitedFolder
var NetworkLimiter *rate.Limiter

// NewDiskLimitReader returns a reader that is rate limited by disk limiter
func NewDiskLimitReader(ctx context.Context, r io.Reader) io.Reader {
	if DiskLimiter == nil {
		return r
	}
	return NewReader(ctx, r, DiskLimiter)
}
