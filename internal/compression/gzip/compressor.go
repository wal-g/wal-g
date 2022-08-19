package gzip

import (
	"compress/gzip"
	"io"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	return gzip.NewWriter(writer)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
