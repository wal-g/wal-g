package testtools

import (
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"io"
	"testing"
)

func MakeDefaultUploader(uploaderAPI s3manageriface.UploaderAPI) *walg.S3Uploader {
	return walg.NewS3Uploader(uploaderAPI, "", "", "STANDARD")
}

func NewMockUploader(apiMultiErr, apiErr bool) *walg.Uploader {
	s3Uploader := MakeDefaultUploader(NewMockS3Uploader(apiMultiErr, apiErr, nil))
	return walg.NewUploader(
		&MockCompressor{},
		walg.NewS3Folder(*s3Uploader, NewMockS3Client(false, true), "bucket/", "server/"),
		nil,
		false,
		false,
	)
}

func NewStoringMockUploader(storage *InMemoryStorage, deltaDataFolder walg.DataFolder) *walg.Uploader {
	return walg.NewUploader(
		&MockCompressor{},
		NewInMemoryStorageFolder("in_memory/", storage),
		deltaDataFolder,
		true,
		true,
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
