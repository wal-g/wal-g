package walg

import (
	"io"
	"os"
)

type ReaderFromWriteCloser interface {
	io.ReaderFrom
	io.WriteCloser
}

type SeekerCloser interface {
	io.Seeker
	io.Closer
}

// ReadCascadeCloser composes io.ReadCloser from two parts
type ReadCascadeCloser struct {
	io.Reader
	io.Closer
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
	_, err = WriteTo(file, content)
	return err
}
