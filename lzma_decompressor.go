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
	buf := make([]byte, 20<<20)
	_, err = lzReader.Read(buf)
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma decompression failed")
	}
	_, err = dst.Write(buf)
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma write failed")
	}
	return nil
}

func (decompressor LzmaDecompressor) FileExtension() string {
	return LzmaFileExtension
}
