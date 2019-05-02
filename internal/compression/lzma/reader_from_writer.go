package lzma

import (
	"github.com/ulikunitz/xz/lzma"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ReaderFromWriter struct {
	lzma.Writer
}

func NewReaderFromWriter(dst io.Writer) (*ReaderFromWriter, error) {
	lzmaWriter, err := lzma.NewWriter(dst)
	if err != nil {
		return nil, err
	}
	return &ReaderFromWriter{
		Writer: *lzmaWriter,
	}, nil
}

func (writer *ReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
