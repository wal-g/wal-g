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
	tracelog.DebugLogger.Printf("request %s response: %d request: %d", r.Method, resp.ContentLength, r.ContentLength)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 && r.Method == "GET" {
		statistics.WalgMetrics.S3BytesRead.Add(float64(resp.ContentLength))
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && (r.Method == "PUT" || r.Method == "POST") {
		statistics.WalgMetrics.S3BytesWritten.Add(float64(r.ContentLength))
	}
	return resp, err
}

func NewRoundTripperWithLogging(old http.RoundTripper) http.RoundTripper {
	if old == nil {
		old = http.DefaultTransport
	}
	return &loggingTransport{underlying: old}
}
