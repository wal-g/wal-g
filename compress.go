package walg

import (
	"github.com/pierrec/lz4"
	"io"
	"os"
	//"fmt"
)

type Lz4CascadeClose struct {
	*lz4.Writer
	underlying io.WriteCloser
}

func (lcc *Lz4CascadeClose) Close() (err error) {
	err = lcc.Writer.Close()
	if err != nil {
		panic(err)
	}
	err = lcc.underlying.Close()
	if err != nil {
		panic(err)
	}
	return
}

type LzWriter interface {
	Writer() io.WriteCloser
}

type FileLzWriter struct {
	chunk io.Reader
	name  string
}

type LzPipeWriter struct {
	chunk io.Reader
	pr    *io.PipeReader
}

func (f *FileLzWriter) Writer() io.WriteCloser {
	flz, err := os.Create(f.name)
	if err != nil {
		panic(err)
	}
	return flz
}

func (p *LzPipeWriter) Writer() io.WriteCloser {
	pr, pw := io.Pipe()
	p.pr = pr
	return pw
}

func (f *FileLzWriter) Compress() {
	w := f.Writer()
	lzw := lz4.NewWriter(w)

	_, err := lzw.ReadFrom(f.chunk)
	if err != nil {
		panic(err)
	}

	if err := lzw.Close(); err != nil {
		panic(err)
	}
	w.Close()

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
