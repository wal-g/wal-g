package sh

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSHFolder(t *testing.T) {
	if os.Getenv("PG_TEST_STORAGE") != "ssh" {
		t.Skip("Credentials needed to run SSH tests")
	}

	st, err := ConfigureStorage(
		// Configuration source docker/pg_tests/scripts/configs/ssh_backup_test_config.json
		fmt.Sprintf("ssh://wal-g_ssh/tmp/sh-folder-test-%x", rand.Int63()),
		map[string]string{
			usernameSetting:       "root",
			portSetting:           "6942",
			privateKeyPathSetting: filepath.ToSlash(path.Join(os.TempDir(), "SSH_KEY")), // run in docker on dev machine or CI
			// PrivateKeyPath: "../../../docker/pg/SSH_KEY", // local manual run on dev machine
		},
	)

	require.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}
