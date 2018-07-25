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
