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
	getter *SplitMaxSizeFactory
	read   int
}

func NewMaxSizeFactory(source io.Reader, maxSize int) SplitMaxSizeFactory {
	return SplitMaxSizeFactory{
		source:   source,
		maxSize:  maxSize,
		isClosed: false,
	}
}

func (s *SplitMaxSizeFactory) GetNewReader() io.Reader {
	if s.isClosed {
		return nil
	}

	return &SplitMaxSizeReader{
		getter: s,
		read:   0,
	}
}

func (sr *SplitMaxSizeReader) Read(buff []byte) (n int, err error) {
	if sr.read == sr.getter.maxSize {
		return 0, io.EOF
	}

	sizeToRead := sr.getter.maxSize - sr.read
	if len(buff) < sizeToRead {
		sizeToRead = len(buff)
	}

	bytes, err := sr.getter.source.Read(buff[:sizeToRead])
	sr.read += bytes

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		sr.getter.isClosed = true
	}

	return bytes, err
}
