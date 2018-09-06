package walg

import (
	"fmt"
	"io"
)

const (
	Lz4AlgorithmName  = "lz4"
	LzmaAlgorithmName = "lzma"
	ZstdAlgorithmName = "zstd"
	BrotliAlgorithmName = "brotli"

	Lz4FileExtension  = "lz4"
	LzmaFileExtension = "lzma"
	ZstdFileExtension = "zst"
	BrotliFileExtension = "br"
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

var Compressors = map[string]Compressor{
	Lz4AlgorithmName:  Lz4Compressor{},
	LzmaAlgorithmName: LzmaCompressor{},
	BrotliAlgorithmName: BrotliCompressor{},
	ZstdAlgorithmName: ZstdCompressor{},
}

var Decompressors = []Decompressor{
	Lz4Decompressor{},
	BrotliDecompressor{},
	LzmaDecompressor{},
	ZstdDecompressor{},
}

func getDecompressorByCompressor(compressor Compressor) Decompressor {
	extension := compressor.FileExtension()
	for _,d:=range Decompressors{
		if d.FileExtension() == extension {
			return d
		}
	}
	return nil
}
