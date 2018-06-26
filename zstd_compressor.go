package walg

import (
	"io"
)

type ZstdCompressor struct {}

func (compressor ZstdCompressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	return NewZstdReaderFromWriter(writer)
}

func (compressor ZstdCompressor) FileExtension() string {
	return ZstdFileExtension
}

