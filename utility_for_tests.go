package walg

import (
	"github.com/pierrec/lz4"
	"io"
)

func NewLz4MockTarUploader() *Uploader {
	return NewUploader("bucket", "server", Lz4AlgorithmName)
}

func NewLz4CompressingPipeWriter(input io.Reader) *CompressingPipeWriter {
	return &CompressingPipeWriter{
		Input: input,
		NewCompressingWriter: func(writer io.Writer) ReaderFromWriteCloser {
			return lz4.NewWriter(writer)
		},
	}
}
