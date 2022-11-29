package internal

import (
	"io"
)

type ReaderWithRetry struct {
	reader        io.ReadCloser
	getReader     func() (io.ReadCloser, error)
	retryAttempts int
	alreadyRead   int
}

func NewReaderWithRetry(getReader func() (io.ReadCloser, error), retryAttempts int) io.ReadCloser {
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	return &ReaderWithRetry{
		reader:        nil,
		getReader:     getReader,
		retryAttempts: retryAttempts,
		alreadyRead:   0,
	}
}

func (r *ReaderWithRetry) setupNewReader() error {
	if r.reader != nil {
		err := r.reader.Close()
		if err != nil {
			return err
		}
	}
	reader, err := r.getReader()
	if err != nil {
		return err
	}
	_, err = io.CopyN(io.Discard, reader, int64(r.alreadyRead))
	r.reader = reader
	return err
}

func (r *ReaderWithRetry) Read(p []byte) (n int, err error) {
	n = 0
	var lastErr error
	for attempt := 0; attempt < r.retryAttempts; attempt++ {
		if r.reader == nil {
			err := r.setupNewReader()
			if err == io.ErrUnexpectedEOF {
				continue
			} else if err != nil {
				return n, err
			}
		}

		read, err := r.reader.Read(p[n:])
		n += read
		r.alreadyRead += read
		lastErr = err
		if err == io.ErrUnexpectedEOF {
			err := r.reader.Close()
			if err != nil {
				return n, err
			}
			r.reader = nil
			continue
		} else if err != nil {
			return n, err
		} else if err == nil || len(p) == n {
			break
		}
	}
	return n, lastErr
}

func (r *ReaderWithRetry) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}
