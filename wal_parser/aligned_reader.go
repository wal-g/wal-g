package wal_parser

import "io"

type AlignedReader struct {
	innerReader io.Reader
	alignment int
	alreadyRead int
}

func NewAlignedReader(source io.Reader, alignment int) *AlignedReader {
	return &AlignedReader{source, alignment, 0}
}

func (reader *AlignedReader) Read(p []byte) (n int, err error) {
	n, err = reader.innerReader.Read(p)
	reader.alreadyRead += n
	return
}

func (reader *AlignedReader) ReadToAlignment() error {
	paddingLength := (reader.alreadyRead + reader.alignment - 1) / reader.alignment * reader.alignment
	padding := make([]byte, paddingLength)
	_, err := reader.innerReader.Read(padding)
	return err
}
