package walg

import (
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"io"
)

/**
 *  Struct that closes lz4 and underlying writer.
 */
type Lz4CascadeClose struct {
	*lz4.Writer
	Underlying io.WriteCloser
}

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

/**
 *  Struct that compresses input into pipe.
 */
type LzPipeWriter struct {
	Input  io.Reader
	Output io.Reader
}

/**
 *  Compresses input using LZ4 to pipe.
 */
func (p *LzPipeWriter) Compress() {
	pr, pw := io.Pipe()
	p.Output = pr
	w := &EmptyWriteIgnorer{pw}
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
					if err = pw.Close(); err != nil {
						e := Lz4Error{errors.Wrap(err, "Compress: lz4 pipe writer close failed")}
						pw.CloseWithError(e)
					}
				}
			}
		}()

	}()

}
