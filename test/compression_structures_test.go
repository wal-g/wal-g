package test

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
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

func GetLz4Compressor() compression.Compressor {
	return compression.Compressors[lz4.AlgorithmName]
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
			WriteCloser: GetLz4Compressor().NewWriter(b),
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
		WriteCloser: GetLz4Compressor().NewWriter(mock),
		Underlying:  mock,
	}

	_, err := lz.Write([]byte{byte('a')})
	assert.Errorf(t, err, "compress: CascadeWriteCloser expected error on write but got `<nil>`")

	err = lz.Close()
	assert.Errorf(t, err, "compress: CascadeWriteCloser expected error on close but got `<nil>`")
}

func TestCompressAndEncrypt(t *testing.T) {
	for _, tt := range tests {
		in := &BufCloser{bytes.NewBufferString(tt.testString), false}
		compressor := GetLz4Compressor()
		compressed := internal.CompressAndEncrypt(in, compressor, nil)

		decompressed := &BufCloser{&bytes.Buffer{}, false}
		decompressor := compression.GetDecompressorByCompressor(compressor)
		err := decompressor.Decompress(decompressed, compressed)
		if err != nil {
			t.Logf("%+v\n", err)
		}

		assert.Equalf(t, tt.testString, decompressed.String(), "compress: CascadeWriteCloser expected '%s' to be written but got '%s'", tt.testString, decompressed)
	}

}

func TestCompressAndEncryptBigChunk(t *testing.T) {
	L := 1024 * 1024 // 1Mb
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(in, compressor, nil)

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	decompressor := compression.GetDecompressorByCompressor(compressor)
	err := decompressor.Decompress(decompressed, compressed)
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

func testCompressAndEncryptErrorPropagation(compressor compression.Compressor, t *testing.T) {
	L := 1 << 20
	b := make([]byte, L)
	rand.Read(b)
	in := &BufCloser{bytes.NewBuffer(b), false}

	compressed := internal.CompressAndEncrypt(in, compressor, nil)

	decompressed := &BufCloser{&bytes.Buffer{}, false}
	decompressor := compression.GetDecompressorByCompressor(compressor)
	err := decompressor.Decompress(decompressed, &DelayedErrorReader{compressed, L})
	assert.Errorf(t, err, "%v did not propagate error of the buffer", compressor.FileExtension())
}

func TestCompressAndEncryptErrorPropagation(t *testing.T) {
	for _, compressor := range compression.Compressors {
		go testCompressAndEncryptErrorPropagation(compressor, t)
	}
}

func TestCompressAndEncryptError(t *testing.T) {
	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(&ErrorReader{}, compressor, nil)

	_, err := ioutil.ReadAll(compressed)
	assert.Errorf(t, err, "compress: CompressingPipeWriter expected error but got `<nil>`")
	if re, ok := err.(internal.CompressAndEncryptError); !ok {
		t.Errorf("compress: CompressingPipeWriter expected CompressAndEncryptError but got %v", re)
	}
}
