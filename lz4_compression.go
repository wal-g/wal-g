package walg

import (
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
)

const Lz4FileExtension = "lz4"

// Lz4CascadeCloser bundles multiple closures
// into one function. Calling Close() will close the
// lz4 and underlying writer.
type Lz4CascadeCloser struct {
	*lz4.Writer
	Underlying io.WriteCloser
}

// Close returns the first encountered error from closing
// the lz4 writer or the underlying writer.
func (lcc *Lz4CascadeCloser) Close() error {
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

// Lz4CascadeCloser2 cascade closers with two independent closers.
// This peculiar behavior is required to handle OpenGPG Writer behavior
type Lz4CascadeCloser2 struct {
	*lz4.Writer
	Underlying  io.WriteCloser
	Underlying2 io.WriteCloser
}

// Close returns the first encountered error from closing
// the lz4 writer or the underlying writers.
func (lcc *Lz4CascadeCloser2) Close() error {
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

// Lz4PipeWriter allows for flexibility of using compressed output.
// Input is read and compressed to a pipe reader.
type Lz4PipeWriter struct {
	Input  io.Reader
	Output io.Reader
}

// Compress compresses input to a pipe reader. Output must be used or
// pipe will block.
func (lzPipeWriter *Lz4PipeWriter) Compress(crypter Crypter) {
	pipeReader, pipeWriter := io.Pipe()
	lzPipeWriter.Output = pipeReader

	var writeCloser io.WriteCloser = pipeWriter
	if crypter.IsUsed() {
		var err error
		writeCloser, err = crypter.Encrypt(pipeWriter)

		if err != nil {
			panic(err)
		}
	}

	writeIgnorer := &EmptyWriteIgnorer{writeCloser}
	lzWriter := lz4.NewWriter(writeIgnorer)

	go func() {
		_, err := lzWriter.ReadFrom(lzPipeWriter.Input)

		if err != nil {
			e := Lz4Error{errors.Wrap(err, "Compress: lz4 compression failed")}
			pipeWriter.CloseWithError(e)
		}

		defer func() {
			if err == nil {
				if err := lzWriter.Close(); err != nil {
					e := Lz4Error{errors.Wrap(err, "Compress: lz4 writer close failed")}
					pipeWriter.CloseWithError(e)
				} else {
					if crypter.IsUsed() {
						err := writeCloser.Close()

						if err != nil {
							e := Lz4Error{errors.Wrap(err, "Compress: encryption failed")}
							pipeWriter.CloseWithError(e)
							return
						}
					}
					if err = pipeWriter.Close(); err != nil {
						e := Lz4Error{errors.Wrap(err, "Compress: lz4 pipe writer close failed")}
						pipeWriter.CloseWithError(e)
					}
				}
			}
		}()

	}()

}

// DecompressLz4 decompresses a .lz4 file. Returns an error upon failure.
func DecompressLz4(d io.Writer, s io.Reader) (int64, error) {
	lz := lz4.NewReader(s)
	n, err := lz.WriteTo(d)
	if err != nil {
		return n, errors.Wrap(err, "DecompressLz4: lz4 write failed")
	}
	return n, nil
}
