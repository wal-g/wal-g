package walg

import (
	"github.com/pierrec/lz4"
	"io"
)

type Lz4CascadeClose struct {
	*lz4.Writer
	Underlying io.WriteCloser
}

func (lcc *Lz4CascadeClose) Close() (err error) {
	err = lcc.Writer.Close()
	if err != nil {
		panic(err)
	}
	err = lcc.Underlying.Close()
	if err != nil {
		panic(err)
	}
	return
}

type LzPipeWriter struct {
	chunk io.Reader
	pr    *io.PipeReader
}

func (p *LzPipeWriter) Writer() io.WriteCloser {
	pr, pw := io.Pipe()
	p.pr = pr
	return pw
}

func (p *LzPipeWriter) Compress() {
	w := p.Writer()
	lzw := lz4.NewWriter(w)

	go func() {
		_, err := lzw.ReadFrom(p.chunk)
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
