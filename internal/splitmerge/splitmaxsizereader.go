package splitmerge

import (
	"io"
)

type SplitMaxSizeFactory struct {
	source   io.Reader
	maxSize  int
	isClosed bool
}

type SplitMaxSizeReader struct {
	factory *SplitMaxSizeFactory
	read    int
}

func NewMaxSizeFactory(source io.Reader, maxSize int) SplitMaxSizeFactory {
	return SplitMaxSizeFactory{
		source:   source,
		maxSize:  maxSize,
		isClosed: false,
	}
}

func (f *SplitMaxSizeFactory) GetNewReader() io.Reader {
	if f.isClosed {
		return nil
	}

	return &SplitMaxSizeReader{
		factory: f,
		read:    0,
	}
}

func (sr *SplitMaxSizeReader) Read(buff []byte) (n int, err error) {
	if sr.read == sr.factory.maxSize {
		return 0, io.EOF
	}

	sizeToRead := sr.factory.maxSize - sr.read
	if len(buff) < sizeToRead {
		sizeToRead = len(buff)
	}

	bytes, err := sr.factory.source.Read(buff[:sizeToRead])
	sr.read += bytes

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		sr.factory.isClosed = true
	} else if _, err := sr.factory.source.Read(make([]byte, 0)); err == io.EOF {
		sr.factory.isClosed = true
	}

	return bytes, err
}
