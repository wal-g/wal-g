package ioextensions_test

import (
	"io"
	"testing"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

type interruptedReader struct {
	sent bool
}

func (r *interruptedReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		p[0] = 'x'
		return 1, nil
	}
	return 0, io.ErrUnexpectedEOF
}

func (r *interruptedReader) Close() error { return nil }

func TestReaderWithRetryPreservesTerminalError(t *testing.T) {
	opened := 0
	reader := ioextensions.NewReaderWithRetry(func() (io.ReadCloser, error) {
		opened++
		return &interruptedReader{}, nil
	}, 2)
	defer reader.Close()

	buf := make([]byte, 2)
	n, err := reader.Read(buf)
	if n != 1 || err != io.ErrUnexpectedEOF {
		t.Fatalf("first read = (%d, %v), want (1, unexpected EOF)", n, err)
	}
	if opened != 2 {
		t.Fatalf("opened %d readers, want 2", opened)
	}

	// A decompressor can consume the byte above and read again. Returning
	// (0, nil) after retries are exhausted makes io.Copy spin forever.
	for i := 0; i < 3; i++ {
		n, err = reader.Read(buf)
		if n != 0 || err != io.ErrUnexpectedEOF {
			t.Fatalf("read after retry exhaustion = (%d, %v), want (0, unexpected EOF)", n, err)
		}
	}
}
