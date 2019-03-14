// +build lzo

package internal

import (
	"io"

	"github.com/cyberdelia/lzo"
)

func NewLzoReader(r io.Reader) (io.ReadCloser, error) {
	return lzo.NewReader(r)
}

func NewLzoWriter(w io.Writer) io.WriteCloser {
	return lzo.NewWriter(w)
}

func init() {
	Decompressors = append(Decompressors, LzoDecompressor{})
}
