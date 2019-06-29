package compression

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/utility"
	"io"
	"math/rand"
	"testing"
)

type BiasedRandomReader struct{}

func NewBiasedRandomReader() *BiasedRandomReader {
	return &BiasedRandomReader{}
}

func (reader *BiasedRandomReader) Read(p []byte) (n int, err error) {
	for i := 0; i < len(p); i++ {
		p[i] = byte(utility.Min(10, rand.Int()%256))
	}
	return len(p), nil
}

func testCompressor(compressor Compressor, testData bytes.Buffer, t *testing.T) {
	initialData := testData
	var compressed bytes.Buffer
	compressingWriter := compressor.NewWriter(&compressed)
	_, err := utility.FastCopy(compressingWriter, &testData)
	assert.NoError(t, err)
	err = compressingWriter.Close()
	assert.NoError(t, err)
	var decompressed bytes.Buffer
	decompressor := GetDecompressorByCompressor(compressor)
	err = decompressor.Decompress(&decompressed, &compressed)
	assert.NoError(t, err)
	assert.Equal(t, initialData.Bytes(), decompressed.Bytes())
}

func TestSmallDataCompression(t *testing.T) {
	const SmallDataSize = 16 << 10
	randomReader := io.LimitReader(NewBiasedRandomReader(), SmallDataSize)
	var testData bytes.Buffer
	io.Copy(&testData, randomReader)
	for _, compressingAlgorithm := range CompressingAlgorithms {
		compressor := Compressors[compressingAlgorithm]
		testCompressor(compressor, testData, t)
	}
}

func TestBigDataCompression(t *testing.T) {
	const BigDataSize = 10 << 20
	randomReader := io.LimitReader(NewBiasedRandomReader(), BigDataSize)
	var testData bytes.Buffer
	io.Copy(&testData, randomReader)
	for _, compressingAlgorithm := range CompressingAlgorithms {
		compressor := Compressors[compressingAlgorithm]
		testCompressor(compressor, testData, t)
	}
}
