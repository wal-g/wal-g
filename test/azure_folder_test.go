package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestAzureFolder(t *testing.T) {
	t.Skip("Credentials needed to run S3 tests")

	storageFolder, err := internal.ConfigureAzureFolder("azure://kubedb/rezoan/sub0")
	assert.NoError(t, err)
	testStorageFolder(storageFolder, t)
}
