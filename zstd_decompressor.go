package walg

import (
	"github.com/DataDog/zstd"
	"io"
)

type ZstdDecompressor struct{}

func (decompressor ZstdDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader := zstd.NewReader(src)
	defer lzReader.Close()
	_, err := io.Copy(dst, lzReader)
	return err
}

func (decompressor ZstdDecompressor) FileExtension() string {
	return ZstdFileExtension
}
