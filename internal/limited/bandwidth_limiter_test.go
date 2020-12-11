package limited_test

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/limited"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	limited.DiskLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	limited.NetworkLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	defer func() {
		limited.DiskLimiter = nil
		limited.NetworkLimiter = nil
	}()
	buffer := bytes.NewReader(make([]byte, 2000))
	r := &fakeCloser{buffer}
	start := utility.TimeNowCrossPlatformLocal()

	reader := limited.NewDiskLimitReader(limited.NewNetworkLimitReader(r))
	_, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	end := utility.TimeNowCrossPlatformLocal()

	if end.Sub(start) < time.Millisecond*80 {
		t.Errorf("Rate limiter did not work")
	}
}
