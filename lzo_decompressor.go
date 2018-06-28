package walg

import (
	"io"
)

const LzopBlockSize = 256 * 1024

type LzoDecompressor struct{}

func (decompressor LzoDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzor, err := NewLzoReader(src)
	if err != nil {
		return err
	}
	defer lzor.Close()

	_, err = io.Copy(dst, lzor)
	return err
}

func (decompressor LzoDecompressor) FileExtension() string {
	return LzoFileExtension
}
