package walparser

import (
	"github.com/pkg/errors"
	"io"
	"fmt"
)

type NotEnoughDataToShrinkError struct {
	error
}

func NewNotEnoughDataToShrinkError(dataRemained int, toShrink int) error {
	return NotEnoughDataToShrinkError{errors.Errorf("not enough data to shrink: dataRemained: %v, toShrink: %v", dataRemained, toShrink)}
}

func (err NotEnoughDataToShrinkError) Error() string {
	return fmt.Sprintf("%+v", err.error)
}

type ShrinkableReader struct {
	innerReader  io.Reader
	dataRemained int
}

func (reader *ShrinkableReader) Read(p []byte) (n int, err error) {
	if reader.dataRemained == 0 {
		return 0, io.EOF
	}
	if len(p) <= reader.dataRemained {
		n, err = reader.innerReader.Read(p)
		reader.dataRemained -= n
		return n, err
	}
	remained := make([]byte, reader.dataRemained)
	n, err = reader.innerReader.Read(remained)
	copy(p, remained)
	reader.dataRemained -= n
	return n, err
}

func (reader *ShrinkableReader) Shrink(length int) error {
	if reader.dataRemained < length {
		return NewNotEnoughDataToShrinkError(reader.dataRemained, length)
	}
	reader.dataRemained -= length
	return nil
}
