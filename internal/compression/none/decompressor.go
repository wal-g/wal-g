package none

import (
	"io"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	if rc, ok := src.(io.ReadCloser); ok {
		return rc, nil
	}

	return io.NopCloser(src), nil
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
