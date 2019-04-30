package internal

import (
	"github.com/wal-g/wal-g/utility"
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

	_, err = fastCopyHandleErrClosedPipe(dst, lzor)
	return err
}

func (decompressor LzoDecompressor) FileExtension() string {
	return LzoFileExtension
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
