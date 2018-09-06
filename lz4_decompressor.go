package walg

import (
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
)

type Lz4Decompressor struct{}

func (decompressor Lz4Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzReader := lz4.NewReader(src)
	_, err := FastCopy(dst, lzReader)
	if err != nil {
		return errors.Wrap(err, "DecompressLz4: lz4 write failed")
	}
	return nil
}

func (decompressor Lz4Decompressor) FileExtension() string {
	return Lz4FileExtension
}
