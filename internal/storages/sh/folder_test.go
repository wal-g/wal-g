package sh

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/storages/storage"
)

func TestSHFolder(t *testing.T) {
	t.Skip("Credentials needed to run SSH tests")

	var storageFolder storage.Folder

	storageFolder, err := ConfigureFolder("ssh://some.host/tmp/x",
		map[string]string{
			Username:       "x4mmm",
			PrivateKeyPath: "/Users/x4mmm/.ssh/id_rsa_pg_tester"})

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
