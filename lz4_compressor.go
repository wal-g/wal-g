package walg

import (
	"io"
	"github.com/pierrec/lz4"
)

type Lz4Compressor struct{}

func (compressor Lz4Compressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	return lz4.NewWriter(writer)
}

func (compressor Lz4Compressor) FileExtension() string {
	return Lz4FileExtension
}
