package walg

import "io"

type ReaderFromWriteCloser interface {
	io.ReaderFrom
	io.WriteCloser
}

type SeekerCloser interface {
	io.Seeker
	io.Closer
}

// ReadCascadeCloser composes io.ReadCloser from two parts
type ReadCascadeCloser struct {
	io.Reader
	io.Closer
}

type NamedReader interface {
	io.Reader
	Name() string
}

type NamedReaderImpl struct {
	io.Reader
	name string
}

func (reader *NamedReaderImpl) Name() string {
	return reader.name
}

type UntilEOFReader struct {
	underlying io.Reader
	isEOF      bool
}

func NewUntilEofReader(underlying io.Reader) *UntilEOFReader {
	return &UntilEOFReader{underlying, false}
}

func (reader *UntilEOFReader) Read(p []byte) (n int, err error) {
	if reader.isEOF {
		return 0, io.EOF
	}
	n, err = reader.underlying.Read(p)
	if err == io.EOF {
		reader.isEOF = true
	}
	return
}
