package oss

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Already-cancelled ctx must abort before any upload, proving ctx reaches the OSS SDK
// rather than being dropped for context.Background().
func TestPutObjectWithContextHonorsCancellation(t *testing.T) {
	config := &Config{
		Region:          "test",
		AccessKeyID:     "test",
		AccessKeySecret: "test",
		Endpoint:        "http://127.0.0.1:0",
		Bucket:          "test-bucket",
		MaxRetries:      1,
		UploadPartSize:  5 * 1024 * 1024,
	}
	client, err := configureClient(config)
	require.NoError(t, err)

	folder := NewFolder(client, config.Bucket, "test-path/", config)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = folder.PutObjectWithContext(ctx, "obj", strings.NewReader("data"))
	require.ErrorIs(t, err, context.Canceled)
}
