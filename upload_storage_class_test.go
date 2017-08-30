// This test file is located within the walg package in order to access the
// unexported createUploadInput function.
package walg

import (
	"testing"
)

func TestUploadInput(t *testing.T) {
	// Test default storage class
	tu := NewTarUploader(nil, "bucket", "server", "region", 1, float64(1))
	input := tu.createUploadInput("path", nil)
	if *input.StorageClass != "STANDARD" {
		t.Errorf("upload: UploadInput field 'StorageClass' expected %s but got %s", "STANDARD", *input.StorageClass)
	}

	// Test STANDARD_IA storage class
	tu = NewTarUploader(nil, "bucket", "server", "region", 1, float64(1))
	tu.StorageClass = "STANDARD_IA"
	input = tu.createUploadInput("path", nil)
	if *input.StorageClass != "STANDARD_IA" {
		t.Errorf("upload: UploadInput field 'StorageClass' expected %s but got %s", "STANDARD_IA", *input.StorageClass)
	}
}
