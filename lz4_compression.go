package walg

import (
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
)

const Lz4FileExtension = "lz4"

// DecompressLz4 decompresses a .lz4 file. Returns an error upon failure.
func DecompressLz4(dst io.Writer, src io.Reader) (int64, error) {
	lzReader := lz4.NewReader(src)
	writtenCount, err := lzReader.WriteTo(dst)
	if err != nil {
		return writtenCount, errors.Wrap(err, "DecompressLz4: lz4 write failed")
	}
	return writtenCount, nil
}
