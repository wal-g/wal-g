package internal

import "io"

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

func newNamedReaderImpl(reader io.Reader, name string) *NamedReaderImpl {
	return &NamedReaderImpl{reader, name}
}
