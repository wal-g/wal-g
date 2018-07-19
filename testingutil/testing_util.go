package testingutil

import (
	"io"
	"testing"
)

func AssertEquals(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
}

func AssertReaderIsEmpty(t *testing.T, reader io.Reader) {
	buf := make([]byte, 1)
	_, err := reader.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected EOF, but got: %v", err)
	}
}

func AssertByteSlicesEqual(t *testing.T, a []byte, b []byte) {
	AssertEquals(t, len(a), len(b))
	for i := range a {
		AssertEquals(t, a[i], b[i])
	}
}
