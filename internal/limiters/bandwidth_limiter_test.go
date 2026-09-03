package limiters_test

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/time/rate"
)

type fakeCloser struct {
	r io.Reader
}

func (r *fakeCloser) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	return n, err
}

func (r *fakeCloser) Close() error {
	return nil
}

func TestLimiter(t *testing.T) {
	limiters.DiskLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	limiters.NetworkLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	defer func() {
		limiters.DiskLimiter = nil
		limiters.NetworkLimiter = nil
	}()
	buffer := bytes.NewReader(make([]byte, 2000))
	r := &fakeCloser{buffer}
	start := utility.TimeNowCrossPlatformLocal()

	reader := limiters.NewDiskLimitReader(limiters.NewNetworkLimitReader(r))
	_, err := io.ReadAll(reader)
	assert.NoError(t, err)
	end := utility.TimeNowCrossPlatformLocal()

	if end.Sub(start) < time.Millisecond*80 {
		t.Errorf("Rate limiter did not work")
	}
}

func TestSetDiskRateLimit_CreatesWhenNil(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	limiters.SetDiskRateLimit(rate.Limit(1000), 2000)

	assert.NotNil(t, limiters.DiskLimiter)
	assert.Equal(t, rate.Limit(1000), limiters.DiskLimiter.Limit())
	assert.Equal(t, 2000, limiters.DiskLimiter.Burst())
}

func TestSetDiskRateLimit_MutatesInPlace(t *testing.T) {
	original := rate.NewLimiter(rate.Limit(1000), 2000)
	limiters.DiskLimiter = original
	defer func() { limiters.DiskLimiter = nil }()

	limiters.SetDiskRateLimit(rate.Limit(5000), 6000)

	assert.Same(t, original, limiters.DiskLimiter)
	assert.Equal(t, rate.Limit(5000), limiters.DiskLimiter.Limit())
	assert.Equal(t, 6000, limiters.DiskLimiter.Burst())
}

func TestSetDiskRateLimit_ConcurrentAccess(t *testing.T) {
	limiters.DiskLimiter = nil
	defer func() { limiters.DiskLimiter = nil }()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			limiters.SetDiskRateLimit(rate.Limit(n+1), int(n+1))
			_ = limiters.NewDiskLimitReader(bytes.NewReader(nil))
		}(int64(i))
	}
	wg.Wait()

	assert.NotNil(t, limiters.DiskLimiter)
}
