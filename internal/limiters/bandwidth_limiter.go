package limiters

import (
	"context"
	"io"
	"sync"

	"golang.org/x/time/rate"
)

var DiskLimiter *rate.Limiter
var NetworkLimiter *rate.Limiter

var diskLimiterMu sync.Mutex

// SetDiskRateLimit creates DiskLimiter if it does not exist yet, or updates the existing
// limiter's limit/burst in place otherwise. This must be used (instead of assigning
// DiskLimiter directly) whenever the value can change after startup, since it may run
// concurrently with readers picking up DiskLimiter via getDiskLimiter/NewDiskLimitReader.
func SetDiskRateLimit(limit rate.Limit, burst int) {
	diskLimiterMu.Lock()
	defer diskLimiterMu.Unlock()
	if DiskLimiter == nil {
		DiskLimiter = rate.NewLimiter(limit, burst)
		return
	}
	DiskLimiter.SetBurst(burst)
	DiskLimiter.SetLimit(limit)
}

func getDiskLimiter() *rate.Limiter {
	diskLimiterMu.Lock()
	defer diskLimiterMu.Unlock()
	return DiskLimiter
}

// NewNetworkLimitReader returns a reader that is rate limited by network limiter
func NewNetworkLimitReader(r io.Reader) io.Reader {
	if NetworkLimiter == nil {
		return r
	}
	return NewReader(context.Background(), r, NetworkLimiter)
}

// NewDiskLimitReader returns a reader that is rate limited by disk limiter
func NewDiskLimitReader(r io.Reader) io.Reader {
	limiter := getDiskLimiter()
	if limiter == nil {
		return r
	}
	return NewReader(context.Background(), r, limiter)
}
