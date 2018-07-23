package walg

import (
	"testing"
	"golang.org/x/time/rate"
	"bytes"
	"io"
	"io/ioutil"
	"time"
)

type fakeCloser struct{
	r io.Reader
}

func TestLimiter(t *testing.T) {
	diskLimiter = rate.NewLimiter(rate.Limit(1000), int(1024));
	networkLimiter = rate.NewLimiter(rate.Limit(1000), int(1024));
	defer func() {
		diskLimiter = nil
		networkLimiter = nil
	}()
	buffer := bytes.NewReader(make([]byte, 2000))
	r:= &fakeCloser{buffer}
	start := time.Now()

	reader := NewDiskLimitReader(NewNetworkLimitReader(r))
	_, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error(err)
		return
	}
	end := time.Now()

	if end.Sub(start) < time.Millisecond * 800 {
		t.Errorf("Rate limiter did not work")
	}
}

func (r *fakeCloser) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	return n, err
}

func (r *fakeCloser) Close() error {
	return nil
}