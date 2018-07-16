package wal_parser

import (
	"io"
	"testing"
)

func assertEquals(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
}

func assertReaderIsEmpty(t *testing.T, reader io.Reader) {
	buf := make([]byte, 1)
	_, err := reader.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, but got: %v", err)
	}
}

func assertByteSlicesEqual(t *testing.T, a []byte, b []byte) {
	assertEquals(t, len(a), len(b))
	for i := range a {
		assertEquals(t, a[i], b[i])
	}
}
