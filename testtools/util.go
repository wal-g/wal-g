package testtools

import (
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"io"
	"testing"
)

func MakeDefaultUploader(uploaderAPI s3manageriface.UploaderAPI) *internal.S3Uploader {
	return internal.NewS3Uploader(uploaderAPI, "", "", "STANDARD")
}

func NewMockUploader(apiMultiErr, apiErr bool) *internal.Uploader {
	s3Uploader := MakeDefaultUploader(NewMockS3Uploader(apiMultiErr, apiErr, nil))
	return internal.NewUploader(
		&MockCompressor{},
		internal.NewS3Folder(*s3Uploader, NewMockS3Client(false, true), "bucket/", "server/"),
		nil,
		false,
		false,
	)
}

func NewStoringMockUploader(storage *InMemoryStorage, deltaDataFolder internal.DataFolder) *internal.Uploader {
	return internal.NewUploader(
		&MockCompressor{},
		NewInMemoryStorageFolder("in_memory/", storage),
		deltaDataFolder,
		true,
		true,
	)
}

func NewLz4CompressingPipeWriter(input io.Reader) *internal.CompressingPipeWriter {
	return &internal.CompressingPipeWriter{
		Input: input,
		NewCompressingWriter: func(writer io.Writer) internal.ReaderFromWriteCloser {
			return internal.NewLz4ReaderFromWriter(writer)
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
	// AB: Go is sick
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
