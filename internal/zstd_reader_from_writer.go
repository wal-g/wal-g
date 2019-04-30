package internal

import (
	"github.com/DataDog/zstd"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ZstdReaderFromWriter struct {
	zstd.Writer
}

func NewZstdReaderFromWriter(dst io.Writer) *ZstdReaderFromWriter {
	zstdWriter := zstd.NewWriterLevel(dst, 3)
	return &ZstdReaderFromWriter{Writer: *zstdWriter}
}

func (writer *ZstdReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
