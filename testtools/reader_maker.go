package testtools

import (
	"io"
	"os"
)

// FileReaderMaker decompresses lzop tarballs from
// the passed in file.
type FileReaderMaker struct {
	Key string
}

func (f *FileReaderMaker) Path() string { return f.Key }

// Reader creates a new reader from the passed in file.
func (f *FileReaderMaker) Reader() (io.ReadCloser, error) {
	r, err := os.Open(f.Key)
	if err != nil {
		return nil, err
	}

	return r, nil
}
