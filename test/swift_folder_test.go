package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestSwiftFolder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	storageFolder, err := internal.ConfigureSwiftFolder("swift://walg-test/temp/sub0")
	assert.NoError(t, err)
	testStorageFolder(storageFolder, t)
}
