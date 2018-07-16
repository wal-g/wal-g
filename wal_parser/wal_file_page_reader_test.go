package wal_parser

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

func TestWalPageReader_ReadPageData(t *testing.T) {
	initialData := make([]byte, WalPageSize)
	for i := 0; i < int(WalPageSize); i++ {
		initialData[i] = byte(rand.Int())
	}
	reader := bytes.NewReader(initialData)
	pageReader := WalPageReader{reader}
	readData, err := pageReader.ReadPageData()
	if err != nil {
		t.Fatalf(err.Error())
	}
	for i := 0; i < int(WalPageSize); i++ {
		if readData[i] != initialData[i] {
			t.Fatalf("initial and read data differs at position: %v, initial is: %v, read is: %v", i, initialData[i], readData[i])
		}
	}
}

func TestWalPageReader_SmallPageError(t *testing.T) {
	initialData := make([]byte, 1000)
	reader := bytes.NewReader(initialData)
	pageReader := WalPageReader{reader}
	_, err := pageReader.ReadPageData()
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("should be ErrUnexpectedEOF, but found %v", err)
	}
}

func TestWalPageReader_EOF(t *testing.T) {
	reader := bytes.NewReader(nil)
	pageReader := WalPageReader{reader}
	_, err := pageReader.ReadPageData()
	if err != io.EOF {
		t.Fatalf("expected EOF error, buf found: %v", err)
	}
}
