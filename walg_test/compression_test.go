package walg_test

import (
	"bytes"
	"github.com/wal-g/wal-g"
	"io"
	"math/rand"
	"testing"
	"github.com/stretchr/testify/assert"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func FindDecompressor(compressorFileExtension string) walg.Decompressor {
	for _, decompressor := range walg.Decompressors {
		if decompressor.FileExtension() == compressorFileExtension {
			return decompressor
		}
	}
	return nil
}

type BiasedRandomReader struct {}

func NewBiasedRandomReader() *BiasedRandomReader {
	return &BiasedRandomReader{}
}

func (reader *BiasedRandomReader) Read(p []byte) (n int, err error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(min(10, rand.Int()%256))
	}
	return len(p), nil
}

func testCompressor(compressor walg.Compressor, testData bytes.Buffer, t *testing.T) {
	initialData := testData
	var compressed bytes.Buffer
	compressingWriter := compressor.NewWriter(&compressed)
	_, err := compressingWriter.ReadFrom(&testData)
	assert.NoError(t, err)
	err = compressingWriter.Close()
	assert.NoError(t, err)
	var decompressed bytes.Buffer
	decompressor := FindDecompressor(compressor.FileExtension())
	err = decompressor.Decompress(&decompressed, &compressed)
	assert.NoError(t, err)
	assert.Equal(t, initialData.Bytes(), decompressed.Bytes())
}

func TestSmallDataCompression(t *testing.T) {
	const SmallDataSize = 16 << 10
	randomReader := io.LimitReader(NewBiasedRandomReader(), SmallDataSize)
	var testData bytes.Buffer
	io.Copy(&testData, randomReader)
	for _, compressingAlgorithm := range walg.CompressingAlgorithms {
		compressor := walg.Compressors[compressingAlgorithm]
		testCompressor(compressor, testData, t)
	}
}

func TestBigDataCompression(t *testing.T) {
	const BigDataSize = 10 << 20
	randomReader := io.LimitReader(NewBiasedRandomReader(), BigDataSize)
	var testData bytes.Buffer
	io.Copy(&testData, randomReader)
	for _, compressingAlgorithm := range walg.CompressingAlgorithms {
		compressor := walg.Compressors[compressingAlgorithm]
		testCompressor(compressor, testData, t)
	}
}
