package blob

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var ErrNoLease = errors.New("no lease")
var ErrNotFound = errors.New("object not found")
var ErrBadRequest = errors.New("invalid request")

type Lease struct {
	ID  string
	End time.Time
}

type DebugResponseWriter struct {
	back http.ResponseWriter
}

func (drw *DebugResponseWriter) Header() http.Header {
	return drw.back.Header()
}

func (drw *DebugResponseWriter) Write(b []byte) (int, error) {
	return drw.back.Write(b)
}

func (drw *DebugResponseWriter) WriteHeader(s int) {
	drw.back.WriteHeader(s)
	b := bytes.NewBuffer([]byte{})
	err := drw.Header().Write(b)
	if err != nil {
		tracelog.ErrorLogger.Printf("WriteHeader failed: %v", err)
	}
	tracelog.DebugLogger.Printf("HTTP %d\n%s\n\n", s, b)
}

type SkipReader struct {
	reader io.Reader
	offset uint64
}

func NewSkipReader(r io.Reader, offset uint64) io.Reader {
	return &SkipReader{r, offset}
}

func (r *SkipReader) Read(s []byte) (int, error) {
	if r.offset > 0 {
		done, err := io.CopyN(io.Discard, r.reader, int64(r.offset))
		if err != nil {
			return 0, err
		}
		if done < int64(r.offset) {
			return 0, io.EOF
		}
		r.offset = 0
	}
	return r.reader.Read(s)
}

const SQLServerCompressionMethod = "sqlserver"

func UseBuiltinCompression() bool {
	method, _ := internal.GetSetting(internal.CompressionMethodSetting)
	return strings.EqualFold(method, SQLServerCompressionMethod)
}
