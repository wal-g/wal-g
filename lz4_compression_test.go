package walg_test

import (
	"bytes"
	"errors"
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
)

type BufCloser struct {
	*bytes.Buffer
	err bool
}

func (w *BufCloser) Close() error {
	if w.err {
		return errors.New("mock close error")
	}
	return nil
}

type ErrorWriteCloser struct{}

func (ew ErrorWriteCloser) Write(p []byte) (int, error) {
	return -1, errors.New("mock writer: write error")
}

func (ew ErrorWriteCloser) Close() error {
	return errors.New("mock writer: close error")
}

type ErrorReader struct{}

func (er ErrorReader) Read(p []byte) (int, error) {
	return -1, errors.New("mock reader: read error")
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
		b := &BufCloser{bytes.NewBufferString(tt.testString), false}
		lz := &walg.Lz4CascadeCloser{
			Writer:     lz4.NewWriter(b),
			Underlying: b,
		}

		random := make([]byte, tt.written)
		_, err := rand.Read(random)
		if err != nil {
			t.Log(err)
		}

		n, err := lz.Write(random)
		if err != nil {
			t.Errorf("compress: Lz4CascadeCloser expected `<nil>` but got %v", err)
		}
		if n != tt.written {
			t.Errorf("compress: Lz4CascadeCloser expected %d bytes written but got %d", tt.written, n)
		}

		err = lz.Close()
		if err != nil {
			t.Errorf("compress: Lz4CascadeCloser expected `<nil>` but got %v", err)
		}

		b.err = true

		err = lz.Close()
		if err == nil {
			t.Errorf("compress: Underlying writer expected to close with error but got `<nil>`")
		}

	}
}

func TestLz4CloseError(t *testing.T) {
	mock := &ErrorWriteCloser{}
	lz := &walg.Lz4CascadeCloser{
		Writer:     lz4.NewWriter(mock),
		Underlying: mock,
	}

	_, err := lz.Write([]byte{byte('a')})
	if err == nil {
		t.Errorf("compress: Lz4CascadeCloser expected error on write but got `<nil>`")
	}

	err = lz.Close()
	if err == nil {
		t.Errorf("compress: Lz4CascadeCloser expected error on close but got `<nil>`")
	}

}

func TestLzPipeWriter(t *testing.T) {
	for _, tt := range tests {
		in := &BufCloser{bytes.NewBufferString(tt.testString), false}
		lz := &walg.Lz4PipeWriter{
			Input: in,
		}

		lz.Compress(walg.MockDisarmedCrypter())

		decompressed := &BufCloser{&bytes.Buffer{}, false}
		_, err := walg.DecompressLz4(decompressed, lz.Output)
		if err != nil {
			t.Logf("%+v\n", err)
		}

		if decompressed.String() != tt.testString {
			t.Errorf("compress: Lz4CascadeCloser expected '%s' to be written but got '%s'", tt.testString, decompressed)
		}
	}

}

func TestLzPipeWriterBigChunk(t *testing.T) {
	L := 1024 * 1024 // 1Mb
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}
	lz := &walg.Lz4PipeWriter{
		Input: in,
	}

	lz.Compress(walg.MockDisarmedCrypter())

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	_, err := walg.DecompressLz4(decompressed, lz.Output)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	if !bytes.Equal(b, decompressed.Bytes()) {
		t.Errorf("Incorrect decompression")
	}

}

type DelayedErrorReader struct {
	underlying io.Reader
	n          int
}

func (er *DelayedErrorReader) Read(p []byte) (int, error) {
	x, err := er.underlying.Read(p)
	if err != nil {
		return -1, err
	}
	er.n -= x
	if er.n < 0 {
		return -1, errors.New("mock reader: read error")
	} else {
		return x, nil
	}
}

func TestLzPipeWriterErrorPropogation(t *testing.T) {
	L := 1024 * 1024 * 4
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}
	lz := &walg.Lz4PipeWriter{
		Input: in,
	}

	lz.Compress(walg.MockDisarmedCrypter())

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	_, err := walg.DecompressLz4(decompressed, &DelayedErrorReader{lz.Output, L})
	if err == nil {
		t.Error("lz4 did not propagate error of the buffer")
	}
}

func TestLzPipeWriterError(t *testing.T) {
	lz := &walg.Lz4PipeWriter{Input: &ErrorReader{}}

	lz.Compress(walg.MockDisarmedCrypter())

	_, err := ioutil.ReadAll(lz.Output)
	if err == nil {
		t.Errorf("compress: Lz4PipeWriter expected error but got `<nil>`")
	}
	if re, ok := err.(walg.Lz4Error); !ok {

		t.Errorf("compress: Lz4PipeWriter expected Lz4Error but got %v", re)
	}
}
