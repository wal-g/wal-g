package internal

import (
	"io"

	"github.com/pkg/errors"
	"github.com/ulikunitz/xz/lzma"
)

type LzmaDecompressor struct{}

func (decompressor LzmaDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader, err := lzma.NewReader(NewUntilEofReader(src))
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma reader creation failed")
	}
	_, err = FastCopy(dst, lzReader)
	return errors.Wrap(err, "DecompressLzma: lzma write failed")
}

func (decompressor LzmaDecompressor) FileExtension() string {
	return LzmaFileExtension
}
