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

var regularTestIncrement = newTestIncrement(sampleLSN)
var smallLsnTestIncrement = newTestIncrement(smallLSN)
var bigLsnTestIncrement = newTestIncrement(bigLSN)

// In this test series we use actual postgres paged file which
// We compute increment with LSN taken from the middle of a file
// Resulting increment is than applied to copy of the same file partially wiped
// Then incremented file is binary compared to the origin
func TestIncrementingFile(t *testing.T) {
	postgresApplyIncrementTest(regularTestIncrement, t)
}

// This test covers the case of empty increment
func TestIncrementingFileBigLSN(t *testing.T) {
	postgresApplyIncrementTest(bigLsnTestIncrement, t)
}

// This test covers the case when increment is bigger than original file
func TestIncrementingFileSmallLSN(t *testing.T) {
	postgresApplyIncrementTest(smallLsnTestIncrement, t)
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

// this test checks that increment is being read correctly
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

func TestReadIncrementFileHeader_Valid(t *testing.T) {
	readIncrementFileHeaderTest(t, internal.IncrementFileHeader, nil)
}

func TestReadIncrementFileHeader_InvalidIncrementFileHeaderError(t *testing.T) {
	dataArray := [][]byte{
		{'w', 'i', '1', 0x56},
		{'x', 'i', '1', internal.SignatureMagicNumber},
		{'w', 'j', '1', internal.SignatureMagicNumber},
	}
	for _, data := range dataArray {
		readIncrementFileHeaderTest(t, data, internal.InvalidIncrementFileHeaderError{})
	}
}

func TestReadIncrementFileHeader_UnknownIncrementFileHeaderError(t *testing.T) {
	readIncrementFileHeaderTest(t, []byte{'w', 'i', '2', internal.SignatureMagicNumber}, internal.UnknownIncrementFileHeaderError{})
}

func readIncrementFileHeaderTest(t *testing.T, headerData []byte, expectedErr error) {
	err := internal.ReadIncrementFileHeader(bytes.NewReader(headerData))
	assert.IsType(t, err, expectedErr)
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

// TestIncrement holds information about some increment for easy testing
type TestIncrement struct {
	incrementBytes []byte
	fileSize uint64
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

