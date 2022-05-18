package lz4

import (
	"io"

	"github.com/pierrec/lz4/v4"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(lz4.NewReader(src)), nil
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
