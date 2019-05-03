package compression

import (
	"github.com/wal-g/wal-g/internal/compression/brotli"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/internal/compression/zstd"
	"io"
)

var CompressingAlgorithms = []string{lz4.AlgorithmName, lzma.AlgorithmName, brotli.AlgorithmName}

type Compressor interface {
	NewWriter(writer io.Writer) io.WriteCloser
	FileExtension() string
}

type Decompressor interface {
	Decompress(dst io.Writer, src io.Reader) error
	FileExtension() string
}

var Compressors = map[string]Compressor{
	lz4.AlgorithmName:    lz4.Compressor{},
	lzma.AlgorithmName:   lzma.Compressor{},
	brotli.AlgorithmName: brotli.Compressor{},
}

var Decompressors = []Decompressor{
	lz4.Decompressor{},
	brotli.Decompressor{},
	lzma.Decompressor{},
	zstd.Decompressor{},
}

func GetDecompressorByCompressor(compressor Compressor) Decompressor {
	return FindDecompressor(compressor.FileExtension())
}

func FindDecompressor(fileExtension string) Decompressor {
	for _, decompressor := range Decompressors {
		if decompressor.FileExtension() == fileExtension {
			return decompressor
		}
	}
	return nil
}
