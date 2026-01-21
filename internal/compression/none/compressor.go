package none

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

const (
	AlgorithmName = "none"
	FileExtension = ""
)

type Compressor struct{}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	return &Writer{writer: writer}
}

type Writer struct {
	writer   io.Writer
	isClosed bool
}

func (w *Writer) Write(p []byte) (n int, err error) {
	if w.isClosed {
		return 0, io.ErrClosedPipe
	}
	return w.writer.Write(p)
}

func (w *Writer) Flush() error {
	return nil
}

func (w *Writer) Close() error {
	if w.isClosed {
		return nil
	}
	w.isClosed = true

	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
