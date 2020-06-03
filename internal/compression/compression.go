package compression

import (
	"io"
)

type Compressor interface {
	NewWriter(writer io.Writer) io.WriteCloser
	FileExtension() string
}

type Decompressor interface {
	Decompress(dst io.Writer, src io.Reader) error
	FileExtension() string
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
