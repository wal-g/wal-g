package walg

import (
	"io"
	"github.com/pkg/errors"
)

// CascadeCloser bundles multiple closures
// into one function. Calling Close() will close the
// main and underlying writers.
type CascadeCloser struct {
	io.WriteCloser
	Underlying io.Closer
}

// Close returns the first encountered error from closing
// main or underlying writer.
func (cascadeCloser *CascadeCloser) Close() error {
	err := cascadeCloser.WriteCloser.Close()
	if err != nil {
		return errors.Wrap(err, "Close: failed to close main writer")
	}
	err = cascadeCloser.Underlying.Close()
	if err != nil {
		return errors.Wrap(err, "Close: failed to close underlying writer")
	}
	return nil
}
