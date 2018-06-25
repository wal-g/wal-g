package walg

import (
	"io"
	"github.com/ulikunitz/xz/lzma"
	"github.com/pkg/errors"
)

type LzmaDecompressor struct{}

func (decompressor LzmaDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader, err := lzma.NewReader(src)
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma reader creation failed")
	}
	_, err = io.Copy(dst, lzReader)
	return err
}

func (decompressor LzmaDecompressor) FileExtension() string {
	return LzmaFileExtension
}
