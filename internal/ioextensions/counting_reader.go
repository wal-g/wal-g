package ioextensions

import "io"

// CountingReader counts the bytes read through it.
//
// It is meant to measure how much data an upload actually pushed to storage, so it should wrap the
// final stream, after compression and encryption. If the storage layer rewinds and re-reads the
// stream to retry a failed upload, the count includes both attempts.
type CountingReader struct {
	reader io.Reader
	read   int64
}

func NewCountingReader(reader io.Reader) *CountingReader {
	return &CountingReader{reader: reader}
}

func (r *CountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}

// BytesRead is only safe to call once the reader is done being read from, which for an upload means
// after the upload call has returned.
func (r *CountingReader) BytesRead() int64 {
	return r.read
}
