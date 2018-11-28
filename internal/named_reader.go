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
