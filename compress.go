package walg

import (
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
)

// Lz4CascadeClose bundles multiple closures
// into one function. Calling Close() will close the
// lz4 and underlying writer.
type Lz4CascadeClose struct {
	*lz4.Writer
	Underlying io.WriteCloser
}

// Close returns the first encountered error from closing
// the lz4 writer or the underlying writer.
func (lcc *Lz4CascadeClose) Close() error {
	err := lcc.Writer.Close()
	if err != nil {
		return errors.Wrap(err, "Lz4 Close: failed to close lz4 writer")
	}
	err = lcc.Underlying.Close()
	if err != nil {
		return errors.Wrap(err, "Lz4 Close: failed to close underlying writer")
	}
	return nil
}

// Cascade closers with two independent closers.
// This peculiar behavior is required to handle OpenGPG Writer behavior
type Lz4CascadeClose2 struct {
	*lz4.Writer
	Underlying  io.WriteCloser
	Underlying2 io.WriteCloser
}

// Close returns the first encountered error from closing
// the lz4 writer or the underlying writer.
func (lcc *Lz4CascadeClose2) Close() error {
	err := lcc.Writer.Close()
	if err != nil {
		return errors.Wrap(err, "Lz4 Close: failed to close lz4 writer")
	}
	err = lcc.Underlying.Close()
	if err != nil {
		return errors.Wrap(err, "Lz4 Close: failed to close underlying writer")
	}
	err = lcc.Underlying2.Close()
	if err != nil {
		return errors.Wrap(err, "Lz4 Close: failed to close underlying writer")
	}
	return nil
}

// LzPipeWriter allows for flexibility of using compressed output.
// Input is read and compressed to a pipe reader.
type LzPipeWriter struct {
	Input  io.Reader
	Output io.Reader
}

// Compress compresses input to a pipe reader. Output must be used or
// pipe will block.
func (p *LzPipeWriter) Compress(crypter Crypter) {
	pr, pw := io.Pipe()
	p.Output = pr

	var wc io.WriteCloser = pw
	if crypter.IsUsed() {
		var err error
		wc, err = crypter.Encrypt(pw)

		if err != nil {
			panic(err)
		}
	}

	w := &EmptyWriteIgnorer{wc}
	lzw := lz4.NewWriter(w)

	go func() {
		_, err := lzw.ReadFrom(p.Input)

		if err != nil {
			e := Lz4Error{errors.Wrap(err, "Compress: lz4 compression failed")}
			pw.CloseWithError(e)
		}

		defer func() {
			if err == nil {
				if err := lzw.Close(); err != nil {
					e := Lz4Error{errors.Wrap(err, "Compress: lz4 writer close failed")}
					pw.CloseWithError(e)
				} else {
					if crypter.IsUsed() {
						err := wc.Close()

						if err != nil {
							e := Lz4Error{errors.Wrap(err, "Compress: encryption failed")}
							pw.CloseWithError(e)
							return
						}
					}
					if err = pw.Close(); err != nil {
						e := Lz4Error{errors.Wrap(err, "Compress: lz4 pipe writer close failed")}
						pw.CloseWithError(e)
					}
				}
			}
		}()

	}()

}
