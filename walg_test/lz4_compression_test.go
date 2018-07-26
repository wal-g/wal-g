package walg_test

import (
	"bytes"
	"errors"
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
)

var MockCloseError = errors.New("mock close error")
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
		lz := &walg.CascadeWriteCloser{
			WriteCloser: lz4.NewWriter(b),
			Underlying:  b,
		}

		random := make([]byte, tt.written)
		_, err := rand.Read(random)
		if err != nil {
			t.Log(err)
		}

		n, err := lz.Write(random)
		if err != nil {
			t.Errorf("compress: CascadeWriteCloser expected `<nil>` but got %v", err)
		}
		if n != tt.written {
			t.Errorf("compress: CascadeWriteCloser expected %d bytes written but got %d", tt.written, n)
		}

		err = lz.Close()
		if err != nil {
			t.Errorf("compress: CascadeWriteCloser expected `<nil>` but got %v", err)
		}

		b.err = true

		err = lz.Close()
		if err == nil {
			t.Errorf("compress: Underlying writer expected to close with error but got `<nil>`")
		}

	}
}

func TestCascadeFileCloserError(t *testing.T) {
	mock := &ErrorWriteCloser{}
	lz := &walg.CascadeWriteCloser{
		WriteCloser: lz4.NewWriter(mock),
		Underlying:  mock,
	}

	_, err := lz.Write([]byte{byte('a')})
	if err == nil {
		t.Errorf("compress: CascadeWriteCloser expected error on write but got `<nil>`")
	}

	err = lz.Close()
	if err == nil {
		t.Errorf("compress: CascadeWriteCloser expected error on close but got `<nil>`")
	}

}

func TestCompressingPipeWriter(t *testing.T) {
	for _, tt := range tests {
		in := &BufCloser{bytes.NewBufferString(tt.testString), false}
		lz := testtools.NewLz4CompressingPipeWriter(in)

		lz.Compress(MockDisarmedCrypter())

		decompressed := &BufCloser{&bytes.Buffer{}, false}
		decompressor := walg.Lz4Decompressor{}
		err := decompressor.Decompress(decompressed, lz.Output)
		if err != nil {
			t.Logf("%+v\n", err)
		}

		if decompressed.String() != tt.testString {
			t.Errorf("compress: CascadeWriteCloser expected '%s' to be written but got '%s'", tt.testString, decompressed)
		}
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
	decompressor := walg.Lz4Decompressor{}
	err := decompressor.Decompress(decompressed, lz.Output)
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

func testCompressingPipeWriterErrorPropagation(compressor walg.Compressor, t *testing.T) error {
	L := 1024 * 1024 * 4
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}
	lz := &walg.CompressingPipeWriter{
		Input: in,
		NewCompressingWriter: func(writer io.Writer) walg.ReaderFromWriteCloser {
			return compressor.NewWriter(writer)
		},
	}

	lz.Compress(MockDisarmedCrypter())

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	decompressor := FindDecompressor(compressor.FileExtension())
	err := decompressor.Decompress(decompressed, &DelayedErrorReader{lz.Output, L})
	return err
}

func TestCompressingPipeWriterErrorPropagation(t *testing.T) {
	for _, compressor := range walg.Compressors {
		err := testCompressingPipeWriterErrorPropagation(compressor, t)
		if err == nil {
			t.Errorf("%v did not propagate error of the buffer", compressor.FileExtension())
		}
	}
}

func TestCompressingPipeWriterError(t *testing.T) {
	lz := testtools.NewLz4CompressingPipeWriter(&ErrorReader{})

	lz.Compress(MockDisarmedCrypter())

	_, err := ioutil.ReadAll(lz.Output)
	if err == nil {
		t.Errorf("compress: CompressingPipeWriter expected error but got `<nil>`")
	}
	if re, ok := err.(walg.CompressingPipeWriterError); !ok {

		t.Errorf("compress: CompressingPipeWriter expected CompressingPipeWriterError but got %v", re)
	}
}
