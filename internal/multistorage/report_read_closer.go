package multistorage

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/multistorage/stats"
)

var _ io.ReadCloser = &reportReadCloser{}

// reportReadCloser wraps an io.ReadCloser and reports the stats.OperationRead result depending on whether the read was
// successful or not.
type reportReadCloser struct {
	io.ReadCloser
	statsCollector stats.Collector
	storage        string
	readBytes      atomic.Int64
	reported       atomic.Bool
}

func newReportReadCloser(readCloser io.ReadCloser, statsCollector stats.Collector, storage string) *reportReadCloser {
	return &reportReadCloser{
		ReadCloser:     readCloser,
		statsCollector: statsCollector,
		storage:        storage,
	}
}

func (r *reportReadCloser) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	r.readBytes.Add(int64(n))
	if err == io.EOF {
		r.reportResult(true)
		return n, err
	}
	if err != nil {
		r.reportResult(false)
		return n, fmt.Errorf("read object content from %q: %w", r.storage, err)
	}
	return n, nil
}

func (r *reportReadCloser) Close() error {
	r.reportResult(true)
	return nil
}

func (r *reportReadCloser) reportResult(success bool) {
	if r.reported.CompareAndSwap(false, true) {
		r.statsCollector.ReportOperationResult(r.storage, stats.OperationRead(r.readBytes.Load()), success)
	}
}
