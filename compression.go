package walg

import (
	"fmt"
	"io"
)

const (
	Lz4AlgorithmName  = "lz4"
	LzmaAlgorithmName = "lzma"
	ZstdAlgorithmName = "zstd"

	Lz4FileExtension  = "lz4"
	LzmaFileExtension = "lzma"
	ZstdFileExtension = "zst"
	LzoFileExtension  = "lzo"
)
var compressingAlgorithms = []string{Lz4AlgorithmName, LzmaAlgorithmName, ZstdAlgorithmName}

type UnknownCompressionMethodError struct{}

func (err UnknownCompressionMethodError) Error() string {
	return fmt.Sprintf("Unkown compression method, supported methods are: %v", compressingAlgorithms)
}

type Compressor interface {
	NewWriter(writer io.Writer) ReaderFromWriteCloser
	FileExtension() string
}

type Decompressor interface {
	Decompress(dst io.Writer, src io.Reader) error
	FileExtension() string
}

var Compressors map[string]Compressor
var Decompressors = []Decompressor{
	Lz4Decompressor{},
	ZstdDecompressor{},
	LzmaDecompressor{},
	LzoDecompressor{},
}

func init() {
	Compressors = make(map[string]Compressor)
	Compressors[Lz4AlgorithmName] = Lz4Compressor{}
	Compressors[LzmaAlgorithmName] = LzmaCompressor{}
	Compressors[ZstdAlgorithmName] = ZstdCompressor{}
}
