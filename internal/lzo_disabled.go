// +build !lzo

package internal

import (
	"io"

	"github.com/wal-g/wal-g/internal/tracelog"
)

func NewLzoReader(r io.Reader) (io.ReadCloser, error) {
	tracelog.ErrorLogger.Fatal("lzo support not compiled into this WAL-G binary")
	return nil, nil
}

func NewLzoWriter(w io.Writer) io.WriteCloser {
	tracelog.ErrorLogger.Fatal("lzo support not compiled into this WAL-G binary")
	return nil
}
