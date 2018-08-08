package walg_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
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

const (
	SmallFilePath        = "./testdata/small_compression_test_data"
	BigFilePath          = "./testdata/big_compression_test_data"
	CompressedFilePath   = "./testdata/compressed_file."
	DecompressedFilePath = "./testdata/decompressed_file"
)

type BiasedRandomReader struct {
	sourceSize int
	bytesRead  int
}

func NewBiasedRandomReader(sourceSize int) *BiasedRandomReader {
	return &BiasedRandomReader{sourceSize, 0}
}

func (reader *BiasedRandomReader) Read(p []byte) (n int, err error) {
	toRead := min(len(p), reader.sourceSize-reader.bytesRead)
	for i := 0; i < toRead; i++ {
		p[i] = byte(min(10, rand.Int()%256))
	}
	reader.bytesRead += toRead
	if toRead < len(p) {
		return toRead, io.EOF
	}
	return toRead, nil
}

type DifferentFileError struct {
	firstFileContent  []byte
	secondFileContent []byte
}

func (err DifferentFileError) Error() string {
	return fmt.Sprintf("Files are different, but should be same, first file content:\n%v\nsecond file content:\n%v\n",
		err.firstFileContent, err.secondFileContent)
}

func compressInitialFile(compressor walg.Compressor, initialFilePath string, t *testing.T) error {
	srcFile, err := os.Open(initialFilePath)
	defer srcFile.Close()
	if err != nil {
		return err
	}
	compressedFile, err := os.Create(CompressedFilePath + compressor.FileExtension())
	defer compressedFile.Close()
	if err != nil {
		return err
	}
	compressingWriter := compressor.NewWriter(compressedFile)
	defer compressingWriter.Close()
	bytesRead, err := compressingWriter.ReadFrom(srcFile)
	t.Logf("Bytes read: %v", bytesRead)
	return err
}

func decompressCompressedFile(compressorFileExtension string) error {
	compressedFile, err := os.Open(CompressedFilePath + compressorFileExtension)
	defer compressedFile.Close()
	if err != nil {
		return err
	}
	decompressedFile, err := os.Create(DecompressedFilePath)
	defer decompressedFile.Close()
	if err != nil {
		return err
	}
	decompressor := FindDecompressor(compressorFileExtension)
	err = decompressor.Decompress(decompressedFile, compressedFile)
	return err
}

func compareDecompressedFileWithInitial(initialFilePath string) error {
	decompressedFileContent, err := ioutil.ReadFile(DecompressedFilePath)
	if err != nil {
		return err
	}
	initialFileContent, err := ioutil.ReadFile(initialFilePath)
	if err != nil {
		return err
	}
	if bytes.Compare(decompressedFileContent, initialFileContent) != 0 {
		return DifferentFileError{
			firstFileContent:  decompressedFileContent,
			secondFileContent: initialFileContent,
		}
	}
	return nil
}

func testCompressor(compressor walg.Compressor, initialFilePath string, t *testing.T) {

	err := compressInitialFile(compressor, initialFilePath, t)
	defer os.Remove(CompressedFilePath + compressor.FileExtension())
	assert.NoError(t, err)

	err = decompressCompressedFile(compressor.FileExtension())
	defer os.Remove(DecompressedFilePath)
	assert.NoError(t, err)

	err = compareDecompressedFileWithInitial(initialFilePath)
	assert.NoError(t, err)
}

func TestSmallFileCompression(t *testing.T) {
	for _, compressingAlgorithm := range walg.CompressingAlgorithms {
		compressor := walg.Compressors[compressingAlgorithm]
		testCompressor(compressor, SmallFilePath, t)
	}
}

func TestBigFileCompression(t *testing.T) {
	const BigFileSize = 50 << 20
	err := generateFile(BigFilePath, BigFileSize)
	assert.NoError(t, err)
	defer os.Remove(BigFilePath)
	for _, compressingAlgorithm := range walg.CompressingAlgorithms {
		compressor := walg.Compressors[compressingAlgorithm]
		testCompressor(compressor, BigFilePath, t)
	}
}
func generateFile(filePath string, size int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	randomReader := NewBiasedRandomReader(size)
	io.Copy(file, randomReader)
	return nil
}
