package internal

import (
	"io"
)

type Lz4Compressor struct{}

func (compressor Lz4Compressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	return NewLz4ReaderFromWriter(writer)
}

func (compressor Lz4Compressor) FileExtension() string {
	return Lz4FileExtension
}
