package internal

import (
	"io"

	"github.com/wal-g/wal-g/internal/limited"
	"golang.org/x/time/rate"
)

var DiskLimiter *rate.Limiter
var NetworkLimiter *rate.Limiter

// NewNetworkLimitReader returns a reader that is rate limited by network limiter
func NewNetworkLimitReader(r io.Reader) io.Reader {
	if NetworkLimiter == nil {
		return r
	}
	return limited.NewReader(r, NetworkLimiter)
}

// NewDiskLimitReader returns a reader that is rate limited by disk limiter
func NewDiskLimitReader(r io.Reader) io.Reader {
	if DiskLimiter == nil {
		return r
	}
	return limited.NewReader(r, DiskLimiter)
}
