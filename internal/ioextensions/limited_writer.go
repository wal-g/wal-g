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
		// Nothing left to hash; pretend we consumed everything so the
		// TeeReader keeps streaming the rest of the file to storage.
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
		// Report the full length so the TeeReader does not treat the
		// discarded tail as a short write.
		return len(p), nil
	}
	return n, nil
}
