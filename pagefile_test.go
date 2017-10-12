package walg

import (
	"testing"
	"fmt"
	"io/ioutil"
	"os"
	"bytes"
	"log"
	"io"
)

const (
	pagedFileName        = "testdata/paged_file.bin"
	sampeLSN      uint64 = 0xc6bd4600
)

// In this test we use actual postgres paged file which
// We compute increment with LSN taken from the middle of a file
// Resulting increment is than applied to copy of the same file partially wiped
// Then incremented file is binary compared to the origin
func TestIncrementingFile(t *testing.T) {
	loclLSN := sampeLSN
	postgresFileTest(loclLSN, t)

}

// This test covers the case of empty increment
func TestIncrementingFileBigLSN(t *testing.T) {
	loclLSN := sampeLSN * 2
	postgresFileTest(loclLSN, t)

}

// This test convers the case when increment is bigger than original file
func TestIncrementingFileSmallLSN(t *testing.T) {
	loclLSN := uint64(0)
	postgresFileTest(loclLSN, t)

}

func postgresFileTest(loclLSN uint64, t *testing.T) {
	reader, isPaged, size, err := ReadDatabaseFile(pagedFileName, &loclLSN, false)
	file, _ := os.Stat(pagedFileName)
	if err != nil {
		fmt.Print(err.Error())
	}
	buf, _ := ioutil.ReadAll(reader)
	if !isPaged {
		t.Error("Sample file is paged")
	}
	if loclLSN != 0 && int64(len(buf)) >= file.Size() {
		t.Error("Increment is too big")
	}

	if loclLSN == 0 && int64(len(buf)) <= file.Size() {
		t.Error("Increment is expected to be bigger than file")
	}
	// We also check that increment correctly predicted it's size
	// This is important for Tar archiver, which writes size in the header
	if int(size) != len(buf) {
		t.Error("Increment has wrong size")
	}
	tmpFileName := pagedFileName + "_tmp"
	CopyFile(pagedFileName, tmpFileName)
	defer os.Remove(tmpFileName)
	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), 477421568-12345)
	tmpFile.Close()
	newReader := bytes.NewReader(buf)
	err = ApplyFileIncrement(tmpFileName, newReader)
	if err != nil {
		t.Error(err)
	}
	_, err = newReader.Read(make([]byte, 1))
	if err != io.EOF {
		t.Error("Not read to the end")
	}
	compare := deepCompare(pagedFileName, tmpFileName)
	if !compare {
		t.Error("Icrement could not restore file")
	}
}

func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
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
	var chunkNumber = 0
	for {

		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

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
