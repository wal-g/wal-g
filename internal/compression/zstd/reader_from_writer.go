package zstd

import (
	"github.com/DataDog/zstd"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ReaderFromWriter struct {
	zstd.Writer
}

func NewReaderFromWriter(dst io.Writer) *ReaderFromWriter {
	zstdWriter := zstd.NewWriterLevel(dst, 3)
	return &ReaderFromWriter{Writer: *zstdWriter}
}

func (writer *ReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
