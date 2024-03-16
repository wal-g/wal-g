package lzma

import (
	"github.com/wal-g/wal-g/internal/ioextensions"
	"io"

	"github.com/ulikunitz/xz/lzma"
)

const (
	AlgorithmName = "lzma"
	FileExtension = "lzma"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	lzmaWriter, err := lzma.NewWriter2(writer)
	if err != nil {
		panic(err)
	}
	return lzmaWriter
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
