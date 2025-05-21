package s3

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/statistics"
)

type MyReadCloser struct {
	underlying io.ReadCloser
	sign       string
}

func (m *MyReadCloser) Close() error {
	return m.underlying.Close()
}

func (m *MyReadCloser) Read(p []byte) (int, error) {
	n, err := m.underlying.Read(p)
	tracelog.DebugLogger.Printf("%s actually read: %d", m.sign, n)
	return n, err
}

type loggingTransport struct {
	underlying http.RoundTripper
}

func (s *loggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = ioutil.ReadAll(r.Body)
		tracelog.DebugLogger.Printf("bytes read1: %d\n", len(body))
		r.Body = &MyReadCloser{underlying: ioutil.NopCloser(bytes.NewBuffer(body)), sign: r.RequestURI}
	}

	resp, err := s.underlying.RoundTrip(r)
	if err != nil {
		return resp, err
	}

	if r.Body != nil {
		n, _ := io.Copy(ioutil.Discard, r.Body)
		tracelog.DebugLogger.Printf("bytes read2: %d\n", n)
	}

	tracelog.DebugLogger.Printf("HTTP response code: %d", resp.StatusCode)
	statistics.WriteStatusCodeMetric(resp.StatusCode)
	tracelog.DebugLogger.Printf("request %s response: %d request: %d", r.Method, resp.ContentLength, r.ContentLength)

	if resp.StatusCode == 400 {
		tracelog.DebugLogger.Printf("request %s content: %d, actual length: %d", r.Method, r.ContentLength, len(body))

	}

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
