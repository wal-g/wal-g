package internal

import (
	"github.com/ulikunitz/xz/lzma"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type LzmaReaderFromWriter struct {
	lzma.Writer
}

func NewLzmaReaderFromWriter(dst io.Writer) (*LzmaReaderFromWriter, error) {
	lzmaWriter, err := lzma.NewWriter(dst)
	if err != nil {
		return nil, err
	}
	return &LzmaReaderFromWriter{
		Writer: *lzmaWriter,
	}, nil
}

func (writer *LzmaReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
