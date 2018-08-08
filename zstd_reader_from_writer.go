package walg

import (
	"github.com/DataDog/zstd"
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
	n, err = FastCopy(writer, reader)
	return
}
