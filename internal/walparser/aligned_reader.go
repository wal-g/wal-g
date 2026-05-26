package walparser

import (
	"io"

	"github.com/pkg/errors"
)

type AlignedReader struct {
	innerReader io.Reader
	alignment   int
	alreadyRead int
	padBuf      []byte
}

func NewAlignedReader(source io.Reader, alignment int) *AlignedReader {
	return &AlignedReader{
		innerReader: source,
		alignment:   alignment,
		padBuf:      make([]byte, alignment),
	}
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
	_, err := reader.Read(reader.padBuf[:paddingLength])
	return errors.WithStack(err)
}
