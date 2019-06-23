package test

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/testtools"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
)

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
		b := &testtools.BufCloser{Buffer: bytes.NewBufferString(tt.testString), Err: false}
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

		b.Err = true

		err = lz.Close()
		assert.Errorf(t, err, "compress: Underlying writer expected to close with error but got `<nil>`")

	}
}

func TestCascadeFileCloserError(t *testing.T) {
	mock := &testtools.ErrorWriteCloser{}
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
		in := &testtools.BufCloser{Buffer: bytes.NewBufferString(tt.testString), Err: false}
		compressor := GetLz4Compressor()
		compressed := internal.CompressAndEncrypt(in, compressor, nil)

		decompressed := &testtools.BufCloser{Buffer: &bytes.Buffer{}, Err: false}
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
	in := &testtools.BufCloser{Buffer: bytes.NewBuffer(b), Err: false}

	compressor := GetLz4Compressor()
	compressed := internal.CompressAndEncrypt(in, compressor, nil)

	decompressed := &testtools.BufCloser{Buffer: &bytes.Buffer{}, Err: false}
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
	in := &testtools.BufCloser{Buffer: bytes.NewBuffer(b), Err: false}

	compressed := internal.CompressAndEncrypt(in, compressor, nil)

	decompressed := &testtools.BufCloser{Buffer: &bytes.Buffer{}, Err: false}
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
	compressed := internal.CompressAndEncrypt(&testtools.ErrorReader{}, compressor, nil)

	_, err := ioutil.ReadAll(compressed)
	assert.Errorf(t, err, "compress: CompressingPipeWriter expected error but got `<nil>`")
	if re, ok := err.(internal.CompressAndEncryptError); !ok {
		t.Errorf("compress: CompressingPipeWriter expected CompressAndEncryptError but got %v", re)
	}
}
