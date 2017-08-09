package tools

import (
	"github.com/pierrec/lz4"
	"io"
	"os"
)

// FileLzWriter takes in input and compresses it to
// local disk.
type FileLzWriter struct {
	Input io.Reader
	Name  string
}

// Writer creates a new FileLzWriter.
func (f *FileLzWriter) Writer() io.WriteCloser {
	flz, err := os.Create(f.Name)
	if err != nil {
		panic(err)
	}
	return flz
}

// Compress uses lz4 to compress bytes to disk.
func (f *FileLzWriter) Compress() {
	w := f.Writer()
	lzw := lz4.NewWriter(w)

	_, err := lzw.ReadFrom(f.Input)
	if err != nil {
		panic(err)
	}

	if err := lzw.Close(); err != nil {
		panic(err)
	}
	w.Close()

}
