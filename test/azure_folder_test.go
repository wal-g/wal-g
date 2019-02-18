package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"os"
	"testing"
)

func TestAzureFolder(t *testing.T) {
	t.Skip("Credentials needed to run Azure Storage tests")

	os.Setenv("AZURE_STORAGE_ACCOUNT", "")
	os.Setenv("AZURE_STORAGE_ACCESS_KEY", "")

	storageFolder, err := internal.ConfigureAzureFolder("azure://test-container/wal-g-test-folder/Sub0")

	assert.NoError(t, err)

	testStorageFolder(storageFolder, t)
}
