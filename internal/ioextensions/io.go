package ioextensions

import (
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
)

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

type ReadSeekCloserImpl struct {
	io.Reader
	io.Seeker
	io.Closer
}

// ReadCascadeCloser composes io.ReadCloser from two parts
type ReadCascadeCloser struct {
	io.Reader
	io.Closer
}

type Flusher interface {
	Flush() error
}

type OnCloseFlusher struct {
	io.WriteCloser
	Flusher
}

func NewOnCloseFlusher(writeCloser io.WriteCloser, flusher Flusher) *OnCloseFlusher {
	return &OnCloseFlusher{writeCloser, flusher}
}

func (cf OnCloseFlusher) Close() error {
	err := cf.WriteCloser.Close()
	if err != nil {
		return err
	}
	return cf.Flush()
}

// ZeroReader generates a slice of zeroes. Used to pad
// tar in cases where length of file changes.
type ZeroReader struct{}

func (z *ZeroReader) Read(p []byte) (int, error) {
	zeroes := make([]byte, len(p))
	n := copy(p, zeroes)
	return n, nil
}

func CreateFileWith(filePath string, content io.Reader) error {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	_, err = utility.FastCopy(file, content)
	return err
}
