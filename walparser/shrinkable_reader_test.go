package walparser

import (
	"bytes"
	"github.com/wal-g/wal-g/walparser/parsingutil/testingutil"
	"testing"
)

func TestShrinkableReader_NotEnoughDataToShrinkError(t *testing.T) {
	reader := ShrinkableReader{nil, 0}
	err := reader.Shrink(5)
	if _, ok := err.(NotEnoughDataToShrinkError); !ok {
		t.Fatalf("expected shrinking error, but got: %v", err)
	}
}

func TestShrinkableReader_Read(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3})
	shrinkableReader := ShrinkableReader{reader, 2}
	buf := make([]byte, 10)
	readCount, err := shrinkableReader.Read(buf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, readCount, 2)
	if buf[0] != 1 || buf[1] != 2 {
		t.Fatalf("read invalid data")
	}
}

func TestShrinkableReader_Shrink(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5})
	shrinkableReader := ShrinkableReader{reader, 4}
	shrinkableReader.Shrink(1)
	buf := make([]byte, 4)
	readCount, err := shrinkableReader.Read(buf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	testingutil.AssertEquals(t, readCount, 3)
}

func TestShrinkableReader_EOF(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3, 4, 5})
	shrinkableReader := ShrinkableReader{reader, 5}
	shrinkableReader.Shrink(5)
	testingutil.AssertReaderIsEmpty(t, &shrinkableReader)
}
