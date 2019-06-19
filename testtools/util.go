package testtools

import (
	"bytes"
	"errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/memory"
	"github.com/wal-g/wal-g/internal/storages/s3"
)

func MakeDefaultInMemoryStorageFolder() *memory.Folder {
	return memory.NewFolder("in_memory/", memory.NewStorage())
}

func MakeDefaultUploader(uploaderAPI s3manageriface.UploaderAPI) *s3.Uploader {
	return s3.NewUploader(uploaderAPI, "", "", "STANDARD")
}

func NewMockUploader(apiMultiErr, apiErr bool) *internal.Uploader {
	s3Uploader := MakeDefaultUploader(NewMockS3Uploader(apiMultiErr, apiErr, nil))
	return internal.NewUploader(
		&MockCompressor{},
		s3.NewFolder(*s3Uploader, NewMockS3Client(false, true), "bucket/", "server/"),
		nil,
	)
}

func NewStoringMockUploader(storage *memory.Storage, deltaDataFolder internal.DataFolder) *internal.Uploader {
	return internal.NewUploader(
		&MockCompressor{},
		memory.NewFolder("in_memory/", storage),
		nil,
	)
}

func CreateMockStorageFolder() storage.Folder {
	var folder = MakeDefaultInMemoryStorageFolder()
	subFolder := folder.GetSubFolder(utility.BaseBackupPath)
	subFolder.PutObject("base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	subFolder.PutObject("base_456_backup_stop_sentinel.json", strings.NewReader("{}"))
	subFolder.PutObject("base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	subFolder.PutObject("base_123312", &bytes.Buffer{})                        // not a sentinel
	subFolder.PutObject("base_321/nop", &bytes.Buffer{})
	subFolder.PutObject("folder123/nop", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/1", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/2", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/3", &bytes.Buffer{})
	return folder
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

type NopCloserWriter struct {
	io.Writer
}

func (NopCloserWriter) Close() error {
	return nil
}

type NopCloser struct{}

func (closer *NopCloser) Close() error {
	return nil
}

type NopSeeker struct{}

func (seeker *NopSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

//ErrorWriter struct implements io.Writer interface.
//Its Write method returns zero and non-nil error on every call
type ErrorWriter struct{}

func (w ErrorWriter) Write(b []byte) (int, error) {
	return 0, errors.New("expected writing error")
}

//ErrorReader struct implements io.Reader interface.
//Its Read method returns zero and non-nil error on every call
type ErrorReader struct{}

func (r ErrorReader) Read(b []byte) (int, error) {
	return 0, errors.New("expected reading error")
}
