package ioextensions

import (
	"io"

	"github.com/wal-g/tracelog"
)

type ReaderWithRetry struct {
	reader        io.ReadCloser
	getReader     func() (io.ReadCloser, error)
	retryAttempts int
	alreadyRead   int
	attempt       int
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
		attempt:       0,
	}
}

func (r *ReaderWithRetry) setupNewReader() error {
	reader, err := r.getReader()
	if err != nil {
		return err
	}
	_, err = io.CopyN(io.Discard, reader, int64(r.alreadyRead))
	r.reader = reader
	return err
}

func (r *ReaderWithRetry) Read(p []byte) (int, error) {
	n := 0
	var lastErr error
	for r.attempt < r.retryAttempts {
		if r.reader == nil {
			err := r.setupNewReader()
			if err == io.EOF {
				return n, err
			} else if err != nil {
				tracelog.ErrorLogger.Printf("error while initializing reader: %v", err)
				tracelog.ErrorLogger.PrintOnError(r.reader.Close())
				r.reader = nil
				r.attempt++
				continue
			}
		}

		read, err := r.reader.Read(p[n:])
		lastErr = err
		n += read
		r.alreadyRead += read

		if err == io.EOF {
			return n, err
		} else if err != nil {
			tracelog.ErrorLogger.Printf("error while read file: %v. Attempt: %d\n", err, r.attempt)
			tracelog.ErrorLogger.PrintOnError(r.reader.Close())
			r.reader = nil
			r.attempt++
			continue
		} else if n == len(p) {
			return n, nil
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
