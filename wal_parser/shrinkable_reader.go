package wal_parser

import (
	"io"
	"fmt"
)

type NotEnoughDataToShrinkError struct {
	dataRemained int
	toShrink int
}

func (err NotEnoughDataToShrinkError) Error() string {
	return fmt.Sprintf("not enough data to shrink: dataRemained: %v, toShrink: %v", err.dataRemained, err.toShrink)
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
		return
	}
	remained := make([]byte, reader.dataRemained)
	n, err = reader.innerReader.Read(remained)
	copy(p, remained)
	reader.dataRemained -= n
	return
}

func (reader *ShrinkableReader) Shrink(length int) error {
	if reader.dataRemained < length {
		return NotEnoughDataToShrinkError{reader.dataRemained, length}
	}
	reader.dataRemained -= length
	return nil
}

