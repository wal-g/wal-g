package compression

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

//go:generate mockgen -destination=../../test/mocks/mock_compressor.go -package mocks -build_flags -mod=readonly github.com/wal-g/wal-g/internal/compression Compressor
type Compressor interface {
	NewWriter(writer io.Writer) ioextensions.WriteFlushCloser
	FileExtension() string
}

type Decompressor interface {
	Decompress(src io.Reader) (io.ReadCloser, error)
	FileExtension() string
}

func GetDecompressorByCompressor(compressor Compressor) Decompressor {
	return FindDecompressor(compressor.FileExtension())
}

func FindDecompressor(fileExtension string) Decompressor {
	// cut the leading '.' (e.g. ".lz4" => "lz4")
	if len(fileExtension) > 0 && fileExtension[0] == '.' {
		fileExtension = fileExtension[1:]
	}

	for _, decompressor := range Decompressors {
		if decompressor.FileExtension() == fileExtension {
			return decompressor
		}
	}
	return nil
}
