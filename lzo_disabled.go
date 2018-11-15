// +build !lzo

package walg

import (
	"io"
)

func NewLzoReader(r io.Reader) (io.ReadCloser, error) {
	errorLogger.Fatal("lzo support not compiled into this WAL-G binary")
	return nil, nil
}

func NewLzoWriter(w io.Writer) io.WriteCloser {
	errorLogger.Fatal("lzo support not compiled into this WAL-G binary")
	return nil
}
