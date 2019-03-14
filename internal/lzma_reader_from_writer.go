package internal

import (
	"io"

	"github.com/ulikunitz/xz/lzma"
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
	n, err = FastCopy(writer, reader)
	return
}
