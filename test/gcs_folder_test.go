package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"os"
	"testing"
)

func TestGSFolder(t *testing.T) {
	t.Skip("Credentials needed to run GCP tests")

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS","/Users/x4mmm/Downloads/mdb-tests-d0uble-0b98813b622b.json")
	storageFolder, err := internal.ConfigureGSFolder("gs://x4m-test/walg-bucket")

	assert.NoError(t, err)

	testStorageFolder(storageFolder, t)
}
