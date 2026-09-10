package ioextensions

import "io"

type LimitedWriter struct {
	w         io.Writer
	remaining int64
}

func NewLimitedWriter(w io.Writer, limit int64) *LimitedWriter {
	return &LimitedWriter{w: w, remaining: limit}
}

func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw.remaining <= 0 {
		return len(p), nil
	}

	toWrite := p
	truncated := false
	if int64(len(p)) > lw.remaining {
		toWrite = p[:lw.remaining]
		truncated = true
	}

	n, err := lw.w.Write(toWrite)
	lw.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	if truncated {
		// Report the full length to avoid a short write error.
		return len(p), nil
	}
	return n, nil
}
