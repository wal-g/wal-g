package internal

import (
	"github.com/pkg/errors"
	"io"
)

// CascadeWriteCloser bundles multiple closures
// into one function. Calling Close() will close the
// main and underlying writers.
type CascadeWriteCloser struct {
	io.WriteCloser
	Underlying io.Closer
}

// Close returns the first encountered error from closing
// main or underlying writer.
func (cascadeCloser *CascadeWriteCloser) Close() error {
	err := cascadeCloser.WriteCloser.Close()
	if err != nil {
		return errors.Wrap(err, "Close: failed to close main writer")
	}
	err = cascadeCloser.Underlying.Close()
	return errors.Wrap(err, "Close: failed to close underlying writer")
}
