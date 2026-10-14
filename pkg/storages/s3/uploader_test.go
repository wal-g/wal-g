package s3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturingUploaderAPI struct {
	body     []byte
	partSize int64
	opts     int
}

func (u *capturingUploaderAPI) Upload(
	_ context.Context,
	input *awss3.PutObjectInput,
	opts ...func(*manager.Uploader),
) (*manager.UploadOutput, error) {
	var err error
	u.body, err = io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	configuration := manager.Uploader{}
	for _, option := range opts {
		option(&configuration)
	}
	u.opts = len(opts)
	u.partSize = configuration.PartSize
	return &manager.UploadOutput{}, nil
}

type testSizedReader struct {
	*bytes.Reader
}

func (r *testSizedReader) RemainingSize() int64 {
	return int64(r.Len())
}

func TestAutomaticPartSizeUsesKnownUploadSize(t *testing.T) {
	api := &capturingUploaderAPI{}
	uploader := NewUploader(api, "", "", "", "STANDARD", "", -1)
	uploader.autoPartSize = true
	uploader.partSize = 5 << 20
	content := []byte("streamed content")

	err := uploader.upload(t.Context(), "bucket", "object", &testSizedReader{bytes.NewReader(content)})
	require.NoError(t, err)
	assert.Equal(t, content, api.body)
	assert.Equal(t, int64(5<<20), api.partSize)
	assert.Equal(t, 1, api.opts)
}

func TestAutomaticPartSizeKeepsUnknownInputStreaming(t *testing.T) {
	api := &capturingUploaderAPI{}
	uploader := NewUploader(api, "", "", "", "STANDARD", "", -1)
	uploader.autoPartSize = true
	uploader.partSize = 5 << 20
	content := []byte("streamed content")

	err := uploader.upload(t.Context(), "bucket", "object", bytes.NewBuffer(content))
	require.NoError(t, err)
	assert.Equal(t, content, api.body)
	assert.Equal(t, 0, api.opts)
}

func TestExplicitPartSizeKeepsStreamingUpload(t *testing.T) {
	api := &capturingUploaderAPI{}
	uploader := NewUploader(api, "", "", "", "STANDARD", "", -1)
	uploader.autoPartSize = false

	err := uploader.upload(t.Context(), "bucket", "object", bytes.NewBufferString("content"))
	require.NoError(t, err)
	assert.Equal(t, 0, api.opts)
}

func TestUploadSizePreservesReadSeekerPosition(t *testing.T) {
	content := bytes.NewReader([]byte("prefix-content"))
	_, err := content.Seek(7, io.SeekStart)
	require.NoError(t, err)

	size, known, err := uploadSize(content)
	require.NoError(t, err)
	assert.True(t, known)
	assert.Equal(t, int64(len("content")), size)

	actual, err := io.ReadAll(content)
	require.NoError(t, err)
	assert.Equal(t, []byte("content"), actual)
}
