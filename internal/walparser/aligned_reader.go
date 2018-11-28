package walparser

import (
	"github.com/pkg/errors"
	"io"
)

type AlignedReader struct {
	innerReader io.Reader
	alignment   int
	alreadyRead int
}

func NewAlignedReader(source io.Reader, alignment int) *AlignedReader {
	return &AlignedReader{source, alignment, 0}
}

func (reader *AlignedReader) Read(p []byte) (n int, err error) {
	n, err = reader.innerReader.Read(p)
	reader.alreadyRead += n
	return n, err
}

func (reader *AlignedReader) ReadToAlignment() error {
	paddingLength := reader.alignment - reader.alreadyRead%reader.alignment
	if paddingLength == reader.alignment {
		return nil
	}
	padding := make([]byte, paddingLength)
	_, err := reader.Read(padding)
	return errors.WithStack(err)
}
