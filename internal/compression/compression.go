package compression

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

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
