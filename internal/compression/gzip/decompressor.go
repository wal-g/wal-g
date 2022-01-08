package gzip

import (
	"compress/gzip"
	"io"
)

type Decompressor struct{}

const FileExtension = "gz"

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(src)
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
