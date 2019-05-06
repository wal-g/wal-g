package lzma

import (
	"github.com/ulikunitz/xz/lzma"
	"io"
)

const (
	AlgorithmName = "lzma"
	FileExtension = "lzma"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	lzmaWriter, err := lzma.NewWriter(writer)
	if err != nil {
		panic(err)
	}
	return lzmaWriter
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
