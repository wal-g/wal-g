package test

import (
	"bytes"
	"errors"
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
)

var MockCloseError = errors.New("mock close: close error")
var MockReadError = errors.New("mock reader: read error")
var MockWriteError = errors.New("mock writer: write error")

type BufCloser struct {
	*bytes.Buffer
	err bool
}

func (w *BufCloser) Close() error {
	if w.err {
		return MockCloseError
	}
	return nil
}

type ErrorWriteCloser struct{}

func (ew ErrorWriteCloser) Write(p []byte) (int, error) {
	return -1, MockWriteError
}

func (ew ErrorWriteCloser) Close() error {
	return MockCloseError
}

type ErrorReader struct{}

func (er ErrorReader) Read(p []byte) (int, error) {
	return -1, MockReadError
}

var tests = []struct {
	testString string
	testLength int
	written    int
}{
	{"testing123456789", 16, 4},
}

func TestCascadeFileCloser(t *testing.T) {
	for _, tt := range tests {
		b := &BufCloser{bytes.NewBufferString(tt.testString), false}
		lz := &internal.CascadeWriteCloser{
			WriteCloser: lz4.NewWriter(b),
			Underlying:  b,
		}

		random := make([]byte, tt.written)
		_, err := rand.Read(random)
		if err != nil {
			t.Log(err)
		}

		n, err := lz.Write(random)
		assert.NoErrorf(t, err, "compress: CascadeWriteCloser expected `<nil>` but got %v", err)
		assert.Equalf(t, n, tt.written, "compress: CascadeWriteCloser expected %d bytes written but got %d", tt.written, n)

		err = lz.Close()
		assert.NoErrorf(t, err, "compress: CascadeWriteCloser expected `<nil>` but got %v", err)

		b.err = true

		err = lz.Close()
		assert.Errorf(t, err, "compress: Underlying writer expected to close with error but got `<nil>`")

	}
}

func TestCascadeFileCloserError(t *testing.T) {
	mock := &ErrorWriteCloser{}
	lz := &internal.CascadeWriteCloser{
		WriteCloser: lz4.NewWriter(mock),
		Underlying:  mock,
	}

	_, err := lz.Write([]byte{byte('a')})
	assert.Errorf(t, err, "compress: CascadeWriteCloser expected error on write but got `<nil>`")

	err = lz.Close()
	assert.Errorf(t, err, "compress: CascadeWriteCloser expected error on close but got `<nil>`")
}

func TestCompressingPipeWriter(t *testing.T) {
	for _, tt := range tests {
		in := &BufCloser{bytes.NewBufferString(tt.testString), false}
		lz := testtools.NewLz4CompressingPipeWriter(in)

		lz.Compress(MockDisarmedCrypter())

		decompressed := &BufCloser{&bytes.Buffer{}, false}
		decompressor := internal.Lz4Decompressor{}
		err := decompressor.Decompress(decompressed, lz.Output)
		if err != nil {
			t.Logf("%+v\n", err)
		}

		assert.Equalf(t, tt.testString, decompressed.String(), "compress: CascadeWriteCloser expected '%s' to be written but got '%s'", tt.testString, decompressed)
	}

}

func TestCompressingPipeWriterBigChunk(t *testing.T) {
	L := 1024 * 1024 // 1Mb
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}
	lz := testtools.NewLz4CompressingPipeWriter(in)

	lz.Compress(MockDisarmedCrypter())

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	decompressor := internal.Lz4Decompressor{}
	err := decompressor.Decompress(decompressed, lz.Output)
	if err != nil {
		t.Logf("%+v\n", err)
	}

	assert.Equalf(t, b, decompressed.Bytes(), "Incorrect decompression")

}

type DelayedErrorReader struct {
	underlying io.Reader
	n          int
}

func (er *DelayedErrorReader) Read(p []byte) (int, error) {
	x, err := er.underlying.Read(p)
	if err != nil {
		return 0, err
	}
	er.n -= x
	if er.n < 0 {
		return 0, errors.New("mock reader: read error")
	} else {
		return x, nil
	}
}

func testCompressingPipeWriterErrorPropagation(compressor internal.Compressor, t *testing.T) {
	L := 1 << 20
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}
	lz := &internal.CompressingPipeWriter{
		Input: in,
		NewCompressingWriter: func(writer io.Writer) internal.ReaderFromWriteCloser {
			return compressor.NewWriter(writer)
		},
	}

	lz.Compress(MockDisarmedCrypter())

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	decompressor := FindDecompressor(compressor.FileExtension())
	err := decompressor.Decompress(decompressed, &DelayedErrorReader{lz.Output, L})
	assert.Errorf(t, err, "%v did not propagate error of the buffer", compressor.FileExtension())
}

func TestCompressingPipeWriterErrorPropagation(t *testing.T) {
	for _, compressor := range internal.Compressors {
		go testCompressingPipeWriterErrorPropagation(compressor, t)
	}
}

func TestCompressingPipeWriterError(t *testing.T) {
	lz := testtools.NewLz4CompressingPipeWriter(&ErrorReader{})

	lz.Compress(MockDisarmedCrypter())

	_, err := ioutil.ReadAll(lz.Output)
	assert.Errorf(t, err, "compress: CompressingPipeWriter expected error but got `<nil>`")
	if re, ok := err.(internal.CompressingPipeWriterError); !ok {
		t.Errorf("compress: CompressingPipeWriter expected CompressingPipeWriterError but got %v", re)
	}
}
