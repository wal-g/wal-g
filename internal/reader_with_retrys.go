package internal

import (
	"io"

	"github.com/wal-g/tracelog"
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
	for attempt := 0; attempt < r.retryAttempts; attempt++ {
		if r.reader == nil {
			err := r.setupNewReader()
			if err == io.EOF {
				return n, err
			} else if err != nil {
				tracelog.ErrorLogger.PrintOnError(r.reader.Close())
				r.reader = nil
				continue
			}
		}

		read, readErr := r.reader.Read(p[n:])
		n += read
		r.alreadyRead += read
		err := readErr

		if err == io.EOF {
			return n, err
		} else if err != nil {
			tracelog.ErrorLogger.Printf("Error while download file: %v. Attempt: %d\n", err, attempt)
			tracelog.ErrorLogger.PrintOnError(r.reader.Close())
			r.reader = nil
			continue
		} else if n == len(p) {
			break
		}
	}

	return n, nil
}

func (r *ReaderWithRetry) Close() error {
	if r.reader != nil {
		return r.reader.Close()
	}
	return nil
}
