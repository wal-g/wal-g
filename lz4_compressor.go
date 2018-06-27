package walg

import (
	"github.com/pierrec/lz4"
	"io"
)

type Lz4Compressor struct{}

func (compressor Lz4Compressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	return lz4.NewWriter(writer)
}

func (compressor Lz4Compressor) FileExtension() string {
	return Lz4FileExtension
}
