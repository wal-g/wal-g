package lzma

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"

	"github.com/ulikunitz/xz/lzma"
)

const (
	AlgorithmName = "lzma"
	FileExtension = "lzma"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	lzmaWriter, err := lzma.NewWriter(writer)
	if err != nil {
		panic(err)
	}
	return Writer{lzmaWriter}
}

type Writer struct {
	*lzma.Writer
}

func (l Writer) Flush() error {
	// Maybe in LZMA2
	panic("Flush not implemented for LZMA.")
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
