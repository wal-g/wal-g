package s3

import (
	"net/http"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/statistics"
)

type loggingTransport struct {
	underlying http.RoundTripper
}

func (s *loggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := s.underlying.RoundTrip(r)
	if err != nil {
		return resp, err
	}

	tracelog.DebugLogger.Printf("HTTP response code: %d", resp.StatusCode)
	statistics.WriteStatusCodeMetric(resp.StatusCode)
	return resp, err
}

func NewRoundTripperWithLogging(old http.RoundTripper) http.RoundTripper {
	if old == nil {
		old = http.DefaultTransport
	}
	return &loggingTransport{underlying: old}
}
