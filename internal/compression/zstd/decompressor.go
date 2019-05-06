package zstd

import (
	"github.com/DataDog/zstd"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	zstdReader := zstd.NewReader(computils.NewUntilEofReader(src))
	_, err := utility.FastCopy(dst, zstdReader)
	if err != nil {
		return errors.Wrap(err, "DecompressZstd: zstd write failed")
	}
	err = zstdReader.Close()
	return errors.Wrap(err, "DecompressZstd: zstd reader close failed")
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
