package testtools

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"io"
	"os"
	"testing"
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

func NewMockTarUploader(apiMultiErr, apiErr bool) *walg.Uploader {
	return walg.NewUploader(
		NewMockS3Uploader(apiMultiErr, apiErr, nil),
		&MockCompressor{},
		walg.NewS3Folder(NewMockS3Client(true, true), "bucket/", "server", false),
		false,
	)
}

func NewStoringMockTarUploader(apiMultiErr, apiErr bool, storage MockStorage) *walg.Uploader {
	return walg.NewUploader(
		NewMockS3Uploader(apiMultiErr, apiErr, storage),
		&MockCompressor{},
		walg.NewS3Folder(nil, "bucket/", "server", false),
		false,
	)
}

func NewLz4CompressingPipeWriter(input io.Reader) *walg.CompressingPipeWriter {
	return &walg.CompressingPipeWriter{
		Input: input,
		NewCompressingWriter: func(writer io.Writer) walg.ReaderFromWriteCloser {
			return walg.NewLz4ReaderFromWriter(writer)
		},
	}
}

func NewMockS3Folder(s3ClientErr, s3ClientNotFound bool) *walg.S3Folder {
	return walg.NewS3Folder(NewMockS3Client(s3ClientErr, s3ClientNotFound), "mock bucket", "mock server", false)
}

func NewStoringMockS3Folder(storage MockStorage) *walg.S3Folder {
	return walg.NewS3Folder(NewMockStoringS3Client(storage), "bucket/", "server", false)
}

type ReadWriteNopCloser struct {
	io.ReadWriter
}

func (readWriteNopCloser *ReadWriteNopCloser) Close() error {
	return nil
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

func AssertReaderIsEmpty(t *testing.T, reader io.Reader) {
	buf := make([]byte, 1)
	_, err := reader.Read(buf)
	assert.Equal(t, io.EOF, err)
}

type NopCloser struct{}

func (closer *NopCloser) Close() error {
	return nil
}

type NopSeeker struct{}

func (seeker *NopSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}
