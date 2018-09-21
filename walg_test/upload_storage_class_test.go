// This test file is located within the walg package in order to access the
// unexported createUploadInput function.
package walg_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestUploadInput(t *testing.T) {
	// Test default storage class
	uploader := testtools.NewMockUploader(false, false)
	input := uploader.CreateUploadInput("path", nil)
	assert.Equal(t, "STANDARD", *input.StorageClass)

	// Test STANDARD_IA storage class
	uploader = testtools.NewMockUploader(false, false)
	uploader.StorageClass = "STANDARD_IA"
	input = uploader.CreateUploadInput("path", nil)
	assert.Equal(t, "STANDARD_IA", *input.StorageClass)
}
