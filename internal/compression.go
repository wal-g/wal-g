package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
)

const (
	Lz4AlgorithmName    = "lz4"
	LzmaAlgorithmName   = "lzma"
	ZstdAlgorithmName   = "zstd"
	BrotliAlgorithmName = "brotli"

	Lz4FileExtension    = "lz4"
	LzmaFileExtension   = "lzma"
	ZstdFileExtension   = "zst"
	BrotliFileExtension = "br"
	LzoFileExtension    = "lzo"
)

var CompressingAlgorithms = []string{Lz4AlgorithmName, LzmaAlgorithmName, BrotliAlgorithmName}

type UnknownCompressionMethodError struct {
	error
}

func NewUnknownCompressionMethodError() UnknownCompressionMethodError {
	return UnknownCompressionMethodError{errors.Errorf("Unknown compression method, supported methods are: %v", CompressingAlgorithms)}
}

func (err UnknownCompressionMethodError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
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
	Lz4AlgorithmName:    Lz4Compressor{},
	LzmaAlgorithmName:   LzmaCompressor{},
	BrotliAlgorithmName: BrotliCompressor{},
}

var Decompressors = []Decompressor{
	Lz4Decompressor{},
	BrotliDecompressor{},
	LzmaDecompressor{},
	ZstdDecompressor{},
}

func getDecompressorByCompressor(compressor Compressor) Decompressor {
	extension := compressor.FileExtension()
	for _, d := range Decompressors {
		if d.FileExtension() == extension {
			return d
		}
	}
	return nil
}
