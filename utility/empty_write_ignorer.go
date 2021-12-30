package utility

import (
	"io"
)

// EmptyWriteIgnorer handles 0 byte write in LZ4 package
// to stop pipe reader/writer from blocking.
type EmptyWriteIgnorer struct {
	io.Writer
}

func (e EmptyWriteIgnorer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return e.Writer.Write(p)
}
