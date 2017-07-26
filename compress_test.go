package walg_test

import (
	"bytes"
	"github.com/katie31/wal-g"
	"github.com/pierrec/lz4"
	"math/rand"
	"testing"
)

type BufCloser struct {
	*bytes.Buffer
}

func (w *BufCloser) Close() error {
	return nil
}

var tests = []struct {
	testString string
	testLength int
	written    int
}{
	{"testing123456789", 16, 4},
}

func TestLz4Close(t *testing.T) {
	for _, tt := range tests {
		b := &BufCloser{bytes.NewBufferString(tt.testString)}
		lz := &walg.Lz4CascadeClose{lz4.NewWriter(b), b}

		random := make([]byte, tt.written)
		_, err := rand.Read(random)
		if err != nil {
			t.Log(err)
		}

		n, err := lz.Write(random)
		if err != nil {
			t.Errorf("compress: Lz4CascadeClose expected `<nil>` but got %v", err)
		}
		if n != tt.written {
			t.Errorf("compress: Lz4CascadeClose expected %d bytes written but got %d", tt.written, n)
		}

		err = lz.Close()
		if err != nil {
			t.Errorf("compress: Lz4CascadeClose expected `<nil>` but got %v", err)
		}
	}
}

func TestLzPipeWriter(t *testing.T) {
	for _, tt := range tests {
		in := &BufCloser{bytes.NewBufferString(tt.testString)}
		lz := &walg.LzPipeWriter{
			Input: in,
		}

		lz.Compress()

		decompressed := &BufCloser{&bytes.Buffer{}}
		walg.DecompressLz4(decompressed, lz.Output)

		if decompressed.String() != tt.testString {
			t.Errorf("compress: Lz4CascadeClose expected %s bytes written but got %s", tt.testString, decompressed)
		}
	}
}
