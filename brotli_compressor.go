package walg

import "io"

type BrotliCompressor struct{}

func (compressor BrotliCompressor) NewWriter(writer io.Writer) ReaderFromWriteCloser {
	return NewBrotliReaderFromWriter(writer)
}

func (compressor BrotliCompressor) FileExtension() string {
	return BrotliFileExtension
}
