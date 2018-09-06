package walg_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"golang.org/x/time/rate"
	"io"
	"io/ioutil"
	"testing"
	"time"
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
	walg.DiskLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	walg.NetworkLimiter = rate.NewLimiter(rate.Limit(10000), int(1024))
	defer func() {
		walg.DiskLimiter = nil
		walg.NetworkLimiter = nil
	}()
	buffer := bytes.NewReader(make([]byte, 2000))
	r := &fakeCloser{buffer}
	start := time.Now()

	reader := walg.NewDiskLimitReader(walg.NewNetworkLimitReader(r))
	_, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	end := time.Now()

	if end.Sub(start) < time.Millisecond*80 {
		t.Errorf("Rate limiter did not work")
	}
}
