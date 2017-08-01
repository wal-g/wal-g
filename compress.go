package walg

import (
	"github.com/pierrec/lz4"
	//"github.com/pkg/errors"
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
		return err
	}
	err = lcc.Underlying.Close()
	if err != nil {
		return err
	}
	return nil
}

/**
 *  Struct that compresses input into pipe
 */
type LzPipeWriter struct {
	Input  io.Reader
	Output *io.PipeReader
}

func (p *LzPipeWriter) Writer() io.WriteCloser {
	pr, pw := io.Pipe()
	p.Output = pr
	return pw
}

func (p *LzPipeWriter) Compress() {
	w := p.Writer()
	lzw := lz4.NewWriter(w)

	go func() {
		_, err := lzw.ReadFrom(p.Input)
		if err != nil {
			panic(err)
		}

		defer func() {
			err := lzw.Close()
			if err != nil {
				panic(err)
			}
			err = w.Close()
			if err != nil {
				panic(err)
			}
		}()

	}()

}
