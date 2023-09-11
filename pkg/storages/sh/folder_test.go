package sh

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSHFolder(t *testing.T) {
	if os.Getenv("PG_TEST_STORAGE") != "ssh" {
		t.Skip("Credentials needed to run SSH tests")
	}

	var storageFolder storage.Folder

	storageFolder, err := ConfigureFolder(
		// Configuration source docker/pg_tests/scripts/configs/ssh_backup_test_config.json
		fmt.Sprintf("ssh://wal-g_ssh/tmp/sh-folder-test-%x", rand.Int63()),
		map[string]string{
			Username:       "root",
			Port:           "6942",
			PrivateKeyPath: "/tmp/SSH_KEY", // run in docker on dev machine or CI
			// PrivateKeyPath: "../../../docker/pg/SSH_KEY", // local manual run on dev machine
		},
	)

	assert.NoError(t, err)
	if t.Failed() {
		return
	}

	storage.RunFolderTest(storageFolder, t)
}
