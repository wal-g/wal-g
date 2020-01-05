package lzma

import (
	"io"

	"github.com/pkg/errors"
	"github.com/ulikunitz/xz/lzma"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"github.com/wal-g/wal-g/utility"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader, err := lzma.NewReader(computils.NewUntilEofReader(src))
	if err != nil {
		return errors.Wrap(err, "DecompressLzma: lzma reader creation failed")
	}
	_, err = utility.FastCopy(dst, lzReader)
	return errors.Wrap(err, "DecompressLzma: lzma write failed")
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
