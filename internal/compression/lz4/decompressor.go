package lz4

import (
	"io"

	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/utility"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader := lz4.NewReader(src)
	_, err := utility.FastCopy(dst, lzReader)
	return errors.Wrap(err, "DecompressLz4: lz4 write failed")
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
