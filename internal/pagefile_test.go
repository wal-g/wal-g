package internal_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
	pagedFileName               = "../test/testdata/base_paged_file.bin"
	pagedFileSizeInBytes        = 65536
	pagedFileBlockCount         = pagedFileSizeInBytes / internal.DatabasePageSize
	sampleLSN            uint64 = 0xc6bd4600
	smallLSN             uint64 = 0
	bigLSN                      = sampleLSN * 2
)

// TestIncrement holds information about some increment for easy testing
type TestIncrement struct {
	incrementBytes []byte
	fileSize       uint64
	diffBlockCount uint32
}

func (ti *TestIncrement) NewReader() io.Reader {
	return bytes.NewReader(ti.incrementBytes)
}

func newTestIncrement(lsn uint64) *TestIncrement {
	incrementBytes := readIncrementToBuffer(lsn)
	fileSize, diffBlockCount, _, _ := internal.GetIncrementHeaderFields(bytes.NewReader(incrementBytes))
	return &TestIncrement{incrementBytes: incrementBytes, fileSize: fileSize, diffBlockCount: diffBlockCount}
}

// in regularTestIncrement backup start LSN is selected so that
// increment contains some of the page file blocks
// with LSN higher than sampleLSN
var regularTestIncrement = newTestIncrement(sampleLSN)

// in allBlocksTestIncrement backup start LSN is very small,
// so the increment contains all of the page file blocks
var allBlocksTestIncrement = newTestIncrement(smallLSN)

// in zeroBlocksTestIncrement backup start LSN is too big,
// so created increment consists of zero blocks
var zeroBlocksTestIncrement = newTestIncrement(bigLSN)

// In this test series we use actual postgres paged file which
// We compute increment with LSN taken from the middle of a file
// Resulting increment is than applied to copy of the same file partially wiped
// Then incremented file is binary compared to the origin
func TestIncrementingFile(t *testing.T) {
	postgresApplyIncrementTest(regularTestIncrement, t)
}

// This test covers the case of empty increment
func TestIncrementingFileZeroBlocksIncrement(t *testing.T) {
	postgresApplyIncrementTest(zeroBlocksTestIncrement, t)
}

// This test covers the case when increment has all blocks of the original file
func TestIncrementingFileAllBlocksIncrement(t *testing.T) {
	postgresApplyIncrementTest(allBlocksTestIncrement, t)
}

func postgresApplyIncrementTest(testIncrement *TestIncrement, t *testing.T) {
	incrementReader := testIncrement.NewReader()
	tmpFileName := pagedFileName + "_tmp"
	copyFile(pagedFileName, tmpFileName)
	defer os.Remove(tmpFileName)
	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), 477421568-12345)
	tmpFile.Close()
	err := internal.ApplyFileIncrement(tmpFileName, incrementReader, false)
	assert.NoError(t, err)
	_, err = incrementReader.Read(make([]byte, 1))
	assert.Equalf(t, io.EOF, err, "Not read to the end")
	compare := deepCompare(pagedFileName, tmpFileName)
	assert.Truef(t, compare, "Increment could not restore file")
}

// In this test series we test that
// increment is being correctly created
// for various LSN
func TestReadingIncrement(t *testing.T) {
	postgresReadIncrementTest(sampleLSN, t)
}

func TestReadingIncrementBigLSN(t *testing.T) {
	postgresReadIncrementTest(bigLSN, t)
}

func TestReadingIncrementSmallLSN(t *testing.T) {
	postgresReadIncrementTest(smallLSN, t)
}

// This test checks that increment is being read correctly
func postgresReadIncrementTest(localLSN uint64, t *testing.T) {
	fileInfo, err := os.Stat(pagedFileName)
	if err != nil {
		fmt.Print(err.Error())
	}
	reader, size, err := internal.ReadIncrementalFile(pagedFileName, fileInfo.Size(), localLSN, nil)
	if err != nil {
		fmt.Print(err.Error())
	}
	buf, _ := ioutil.ReadAll(reader)
	assert.Falsef(t, localLSN != 0 && int64(len(buf)) >= fileInfo.Size(), "Increment is too big")

	assert.Falsef(t, localLSN == 0 && int64(len(buf)) <= fileInfo.Size(), "Increment is expected to be bigger than file")
	// We also check that increment correctly predicted it's size
	// This is important for Tar archiver, which writes size in the header
	assert.Equalf(t, len(buf), int(size), "Increment has wrong size")
}

// Header of the correct increment file should be read without errors
func TestReadIncrementFileHeader_Valid(t *testing.T) {
	readIncrementFileHeaderTest(t, internal.IncrementFileHeader, nil)
}

// Should return InvalidIncrementFileHeaderError
// when reading increment file with invalid header
func TestReadIncrementFileHeader_InvalidIncrementFileHeaderError(t *testing.T) {
	// Valid WAL-G increment header should start with "wi(some digit)SignatureMagicNumber"
	// more info: https://github.com/wal-g/wal-g/blob/01911090ba1eef305aa87f06d3f8cf20e3524d9a/internal/incremental_page_reader.go#L16
	dataArray := [][]byte{
		{'w', 'i', '1', 0x56},
		{'x', 'i', '1', internal.SignatureMagicNumber},
		{'w', 'j', '1', internal.SignatureMagicNumber},
	}
	for _, data := range dataArray {
		readIncrementFileHeaderTest(t, data, internal.InvalidIncrementFileHeaderError{})
	}
}

// Should return UnknownIncrementFileHeaderError
// when reading increment with not supported header version
func TestReadIncrementFileHeader_UnknownIncrementFileHeaderError(t *testing.T) {
	readIncrementFileHeaderTest(t, []byte{'w', 'i', '2', internal.SignatureMagicNumber}, internal.UnknownIncrementFileHeaderError{})
}

func readIncrementFileHeaderTest(t *testing.T, headerData []byte, expectedErr error) {
	err := internal.ReadIncrementFileHeader(bytes.NewReader(headerData))
	assert.IsType(t, err, expectedErr)
}

// In this test series we test that new page file
// is being correctly created from increment file
// with different increment cases
func TestCreatingFileFromIncrement(t *testing.T) {
	postgresCreateFileFromIncrementTest(regularTestIncrement, t)
}

func TestCreatingFileFromZeroBlocksIncrement(t *testing.T) {
	postgresCreateFileFromIncrementTest(zeroBlocksTestIncrement, t)
}

func TestCreatingFileFromAllBlocksIncrement(t *testing.T) {
	postgresCreateFileFromIncrementTest(allBlocksTestIncrement, t)
}

func postgresCreateFileFromIncrementTest(testIncrement *TestIncrement, t *testing.T) {
	incrementReader := testIncrement.NewReader()
	mockFile := NewMockReadWriterAt(make([]byte, 0))

	err := internal.CreateFileFromIncrement(incrementReader, mockFile)
	assert.NoError(t, err, "Expected no errors after creating file from increment")
	assert.Equal(t, testIncrement.fileSize, uint64(len(mockFile.content)),
		"Result file size should match the size specified in the increment header")

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")

	checkAllWrittenBlocksCorrect(mockFile, sourceFile, testIncrement.diffBlockCount, t)
}

// In this test series we test that
// no increment blocks are being written if the
// local file is completed (no missing blocks)
func TestWritingIncrementToCompletedFile(t *testing.T) {
	postgresWriteIncrementTestCompletedFile(regularTestIncrement, t)
}

func TestWritingAllBlocksIncrementToCompletedFile(t *testing.T) {
	postgresWriteIncrementTestCompletedFile(allBlocksTestIncrement, t)
}

func TestWritingZeroBlocksIncrementToCompletedFile(t *testing.T) {
	postgresWriteIncrementTestCompletedFile(zeroBlocksTestIncrement, t)
}

func postgresWriteIncrementTestCompletedFile(testIncrement *TestIncrement, t *testing.T) {
	mockContent, _ := ioutil.ReadFile(pagedFileName)
	mockFile := NewMockReadWriterAt(mockContent)

	err := internal.WritePagesFromIncrement(testIncrement.NewReader(), mockFile, false)

	assert.NoError(t, err, "Expected no errors after writing increment")
	// check that no bytes were written to the mock file
	assert.Equal(t, 0, mockFile.bytesWritten,
		"No bytes should be written since the file is complete")
}

// In this test series we test that
// all increment blocks are being written if the
// local file is empty (all blocks are missing)
func TestWritingIncrementToEmptyFile(t *testing.T) {
	postgresWritePagesTestEmptyFile(regularTestIncrement, t)
}

func TestWritingAllBlocksIncrementToEmptyFile(t *testing.T) {
	postgresWritePagesTestEmptyFile(allBlocksTestIncrement, t)
}

func TestWritingZeroBlocksIncrementToEmptyFile(t *testing.T) {
	postgresWritePagesTestEmptyFile(zeroBlocksTestIncrement, t)
}

func postgresWritePagesTestEmptyFile(testIncrement *TestIncrement, t *testing.T) {
	mockContent := make([]byte, internal.DatabasePageSize*pagedFileBlockCount)
	mockFile := NewMockReadWriterAt(mockContent)
	err := internal.WritePagesFromIncrement(testIncrement.NewReader(), mockFile, false)
	assert.NoError(t, err, "Expected no errors after writing increment")
	assert.Equal(t, testIncrement.fileSize, uint64(len(mockFile.content)),
		"Result file size should match the size specified in the increment header")

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	checkAllWrittenBlocksCorrect(mockFile, sourceFile, testIncrement.diffBlockCount, t)
}

// Increment blocks should be written in places of missing blocks
// of the partially completed local file. Using all blocks increment
// ensures that each missing block exist in increment.
// In this test case, after applying the increment,
// we should get the completed page file
func TestWritingIncrementToIncompleteFile(t *testing.T) {
	incrementReader := allBlocksTestIncrement.NewReader()
	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	mockContent, _ := ioutil.ReadFile(pagedFileName)
	for i := pagedFileSizeInBytes / 2; i < len(mockContent); i++ {
		mockContent[i] = 0
	}
	mockFile := NewMockReadWriterAt(mockContent)

	err := internal.WritePagesFromIncrement(incrementReader, mockFile, false)

	assert.NoError(t, err, "Expected no errors after writing increment")
	assert.Equal(t, allBlocksTestIncrement.fileSize, uint64(len(mockFile.content)),
		"Result file size should match the size specified in the increment header")

	mockFileReader := bytes.NewReader(mockFile.content)
	compareResult := deepCompareReaders(sourceFile, mockFileReader)
	assert.Truef(t, compareResult, "Increment could not restore file")
}

// No bytes should be written if the file is complete (no missing blocks)
func TestRestoringPagesToCompletedFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	fileReader := io.Reader(pagedFile)
	defer utility.LoggedClose(pagedFile, "")
	mockContent, _ := ioutil.ReadFile(pagedFileName)
	mockFile := NewMockReadWriterAt(mockContent)

	err := internal.RestoreMissingPages(fileReader, mockFile)

	assert.NoError(t, err, "Expected no errors after restoring missing pages")
	// check that no bytes were written to the mock file
	assert.Equal(t, 0, mockFile.bytesWritten,
		"No bytes should be written since the file is complete")
}

// Missing blocks should be filled with pages from the local file
func TestRestoringPagesToIncompleteFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	fileReader := io.Reader(pagedFile)
	defer utility.LoggedClose(pagedFile, "")
	mockContent, _ := ioutil.ReadFile(pagedFileName)
	for i := pagedFileSizeInBytes / 2; i < len(mockContent); i++ {
		mockContent[i] = 0
	}
	mockFile := NewMockReadWriterAt(mockContent)

	err := internal.RestoreMissingPages(fileReader, mockFile)

	assert.NoError(t, err, "Expected no errors after restoring missing pages")
	pagedFile.Seek(0, 0)
	mockFileReader := bytes.NewReader(mockFile.content)
	compareResult := deepCompareReaders(pagedFile, mockFileReader)
	assert.Truef(t, compareResult, "Increment could not restore file")
}

// If the local file is empty, it should be fully restored
func TestRestoringPagesToEmptyFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	fileReader := io.Reader(pagedFile)
	defer utility.LoggedClose(pagedFile, "")
	mockContent := make([]byte, internal.DatabasePageSize*pagedFileBlockCount)
	mockFile := NewMockReadWriterAt(mockContent)

	err := internal.RestoreMissingPages(fileReader, mockFile)

	assert.NoError(t, err, "Expected no errors after restoring missing pages")
	mockFileReader := bytes.NewReader(mockFile.content)
	pagedFile.Seek(0, 0)
	compareResult := deepCompareReaders(pagedFile, mockFileReader)
	assert.Truef(t, compareResult, "Increment could not restore file")
}

// Verify that all increment blocks exist in the resulting file
// and that each block has been written to the right place
func checkAllWrittenBlocksCorrect(mockFile *MockReadWriterAt, sourceFile io.ReaderAt,
	diffBlockCount uint32, t *testing.T) {
	emptyPage := make([]byte, internal.DatabasePageSize)
	dataBlockCount := uint32(0)

	for index, data := range mockFile.getBlocks() {
		readBytes := make([]byte, internal.DatabasePageSize)
		sourceFile.ReadAt(readBytes, index*internal.DatabasePageSize)
		if bytes.Equal(emptyPage, data) {
			continue
		}
		// Verify that each written block corresponds
		// to the actual block in the original page file.
		assert.True(t, bytes.Equal(readBytes, data), "Result file is incorrect")
		dataBlockCount += 1
	}
	// Make sure that we wrote exactly the same amount of blocks that was written to the increment header.
	// These two numbers may not be equal if the increment was taken
	// while the database cluster was running, but in this test cases they should match.
	assert.Equal(t, diffBlockCount, dataBlockCount, "Result file is incorrect")
}

func readIncrementToBuffer(localLSN uint64) []byte {
	fileInfo, _ := os.Stat(pagedFileName)
	reader, _, _ := internal.ReadIncrementalFile(pagedFileName, fileInfo.Size(), localLSN, nil)
	buf, _ := ioutil.ReadAll(reader)
	return buf
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(in, "")

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(out, "")

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return nil
}

const chunkSize = 64

func deepCompare(file1, file2 string) bool {
	// Check file size ...

	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}
	return deepCompareReaders(f1, f2)
}

func deepCompareReaders(r1, r2 io.Reader) bool {
	var chunkNumber = 0
	for {
		b1 := make([]byte, chunkSize)
		_, err1 := r1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := r2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			log.Printf("Bytes at %v differ\n", chunkNumber*chunkSize)
			log.Println(b1)
			log.Println(b2)
			return false
		}
		chunkNumber++
	}
}

// MockReadWriterAt used for mocking file in tests
type MockReadWriterAt struct {
	size         int64
	bytesWritten int
	content      []byte
}

func NewMockReadWriterAt(content []byte) *MockReadWriterAt {
	return &MockReadWriterAt{size: int64(len(content)), content: content}
}

func (mrw *MockReadWriterAt) WriteAt(b []byte, offset int64) (n int, err error) {
	bytesCount := uint64(offset) + uint64(len(b))
	for uint64(len(mrw.content)) < bytesCount {
		// silly slice expand method but for tests seems fine
		mrw.content = append(mrw.content, 0)
	}
	copy(mrw.content[offset:], b)
	mrw.bytesWritten += len(b)
	return len(b), nil
}

// get mock file content represented as page blocks
func (mrw *MockReadWriterAt) getBlocks() map[int64][]byte {
	totalBlockCount := int64(len(mrw.content)) / internal.DatabasePageSize
	result := make(map[int64][]byte, totalBlockCount)
	for i := int64(0); i < totalBlockCount; i++ {
		result[i] = make([]byte, internal.DatabasePageSize)
		_, _ = mrw.ReadAt(result[i], i*internal.DatabasePageSize)
	}
	return result
}

func (mrw *MockReadWriterAt) ReadAt(b []byte, offset int64) (n int, err error) {
	block := mrw.content[offset : offset+int64(len(b))]
	copy(b, block)
	return len(block), nil
}

func (mrw *MockReadWriterAt) Size() int64 {
	return mrw.size
}

func (mrw *MockReadWriterAt) Name() string {
	return "mock_file"
}
