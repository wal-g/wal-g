package testtools

import (
	"io"
	"os"

	"github.com/wal-g/wal-g/internal"
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

func (f *FileReaderMaker) FileType() internal.FileType {
	return internal.TarFileType
}

func (f *FileReaderMaker) Mode() int {
	return 0
}
