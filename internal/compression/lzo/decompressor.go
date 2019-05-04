// +build lzo

package lzo

import (
	"github.com/cyberdelia/lzo"
	"github.com/wal-g/wal-g/utility"
	"io"
)

const (
	FileExtension = "lzo"

	LzopBlockSize = 256 * 1024
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	lzor, err := lzo.NewReader(src)
	if err != nil {
		return err
	}
	defer lzor.Close()

	_, err = fastCopyHandleErrClosedPipe(dst, lzor)
	return err
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}

func fastCopyHandleErrClosedPipe(dst io.Writer, src io.Reader) (int64, error) {
	n := int64(0)
	buf := make([]byte, utility.CompressedBlockMaxSize)
	for {
		read, readingErr := src.Read(buf)
		if readingErr != nil && readingErr != io.EOF {
			return n, readingErr
		}
		written, writingErr := dst.Write(buf[:read])
		n += int64(written)
		if writingErr == io.ErrClosedPipe {
			// Here we handle LZO padded with zeroes:
			// writer cannot consume anymore data, but all we have is zeroes
			for {
				if !utility.AllZero(buf[written:read]) {
					return n, writingErr
				}
				if readingErr == io.EOF {
					return n, nil
				}
				read, readingErr = src.Read(buf)
				if readingErr != nil && readingErr != io.EOF {
					return n, readingErr
				}
				written = 0
			}
		}
		if writingErr != nil || readingErr == io.EOF {
			return n, writingErr
		}
	}
}
