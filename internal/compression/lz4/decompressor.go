package lz4

import (
	"io"
	"io/ioutil"

	"github.com/pierrec/lz4/v4"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	return ioutil.NopCloser(lz4.NewReader(src)), nil
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
