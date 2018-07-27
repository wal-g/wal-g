package testtools

import (
	"fmt"
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g"
	"io"
	"os"
	"time"
)

// MakeDir creates a new directory with mode 0755.
func MakeDir(name string) {
	dest := name
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, 0755); err != nil {
			panic(err)
		}
	}
}

// TimeTrack is used to time how long functions take.
//
// Usage Example:
// defer timeTrack(time.Now(), "EXTRACT ALL")
func TimeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	fmt.Printf("%s took %s\n", name, elapsed)
}

func NewLz4MockTarUploader() *walg.Uploader {
	return walg.NewUploader(walg.Lz4AlgorithmName, walg.NewS3Folder(nil, "bucket", "server"))
}

func NewLz4CompressingPipeWriter(input io.Reader) *walg.CompressingPipeWriter {
	return &walg.CompressingPipeWriter{
		Input: input,
		NewCompressingWriter: func(writer io.Writer) walg.ReaderFromWriteCloser {
			return lz4.NewWriter(writer)
		},
	}
}

func NewMockS3Folder(s3ClientErr, s3ClientNotFound bool) *walg.S3Folder {
	return walg.NewS3Folder(NewMockS3Client(s3ClientErr, s3ClientNotFound), "mock bucket", "mock server")
}

func Contains(s *[]string, e string) bool {
	//AB: Go is sick
	if s == nil {
		return false
	}
	for _, a := range *s {
		if a == e {
			return true
		}
	}
	return false
}
