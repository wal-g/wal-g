package walg

import (
	"io"
	"github.com/pierrec/lz4"
)

func NewLz4MockTarUploader() *TarUploader {
	return NewTarUploader("bucket", "server", Lz4AlgorithmName)
}

func NewLz4CompressingPipeWriter(input io.Reader) *CompressingPipeWriter {
	return &CompressingPipeWriter{
		Input: input,
		NewCompressingWriter: func(writer io.Writer) ReaderFromWriteCloser {
			return lz4.NewWriter(writer)
		},
	}
}
