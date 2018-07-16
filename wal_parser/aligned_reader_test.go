package wal_parser

import (
	"bytes"
	"io"
	"testing"
)

func TestAlignedReader_EOF(t *testing.T) {
	reader := bytes.NewReader(nil)
	alignedReader := NewAlignedReader(reader, 2)
	assertReaderIsEmpty(t, alignedReader)
}

func TestAlignedReader_Read(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4})
	alignedReader := NewAlignedReader(reader, 2)
	buf := make([]byte, 3)
	n, err := io.ReadFull(alignedReader, buf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, n, alignedReader.alreadyRead)
	if buf[0] != 1 || buf[1] != 2 || buf[2] != 3 {
		t.Fatalf("incorrect data was read")
	}
}

func TestAlignedReader_ReadAfterAlignment(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
	alignedReader := NewAlignedReader(reader, 3)
	buf := make([]byte, 4)
	alignedReader.Read(buf)
	alignedReader.ReadToAlignment()
	readCount, err := alignedReader.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf(err.Error())
	}
	assertEquals(t, readCount, 3)
	if buf[0] != 7 || buf[1] != 8 || buf[2] != 9 {
		t.Fatalf("incorrect data was read")
	}
}

func TestAlignedReader_ReadToAlignment(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5})
	alignedReader := NewAlignedReader(reader, 2)
	buf := make([]byte, 3)
	alignedReader.Read(buf)
	err := alignedReader.ReadToAlignment()
	if err != nil {
		t.Fatalf(err.Error())
	}
	assertEquals(t, alignedReader.alreadyRead, 4)
}
