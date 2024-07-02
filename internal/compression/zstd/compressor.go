package zstd

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"

	"github.com/klauspost/compress/zstd"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	zw, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(err)
	}

	return zw
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
