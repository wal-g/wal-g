package zstd

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	zw, err := zstd.NewWriter(writer, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(err)
	}

	return zw
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
