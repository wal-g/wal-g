//go:build lzo
// +build lzo

package lzo

import (
	"io"
	"io/ioutil"

	"github.com/cyberdelia/lzo"
)

const (
	FileExtension = "lzo"

	LzopBlockSize = 256 * 1024
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	lzor, err := lzo.NewReader(src)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(lzor), nil
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
