// This test file is located within the walg package in order to access the
// unexported createUploadInput function.
package walg_test

import (
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestUploadInput(t *testing.T) {
	// Test default storage class
	tu := testtools.NewLz4MockTarUploader()
	input := tu.CreateUploadInput("path", nil)
	if *input.StorageClass != "STANDARD" {
		t.Errorf("upload: UploadInput field 'StorageClass' expected %s but got %s", "STANDARD", *input.StorageClass)
	}

	// Test STANDARD_IA storage class
	tu = testtools.NewLz4MockTarUploader()
	tu.StorageClass = "STANDARD_IA"
	input = tu.CreateUploadInput("path", nil)
	if *input.StorageClass != "STANDARD_IA" {
		t.Errorf("upload: UploadInput field 'StorageClass' expected %s but got %s", "STANDARD_IA", *input.StorageClass)
	}
}
