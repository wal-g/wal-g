package ioextensions

import "io"

type NamedReader interface {
	io.Reader
	Name() string
	IsExactPath() bool
}

type NamedReaderImpl struct {
	io.Reader
	name string
	isExactPath bool
}

func (reader *NamedReaderImpl) Name() string {
	return reader.name
}

func (reader *NamedReaderImpl) IsExactPath() bool {
	return reader.isExactPath
}

func NewNamedReaderImpl(reader io.Reader, name string) *NamedReaderImpl {
	return &NamedReaderImpl{reader, name, false}
}

func NewNamedReaderExactPathImpl(reader io.Reader, name string) *NamedReaderImpl {
	return &NamedReaderImpl{reader, name, true}
}
