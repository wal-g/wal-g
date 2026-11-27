package sh

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSHFolder(t *testing.T) {
	if os.Getenv("PG_TEST_STORAGE") != "ssh" {
		t.Skip("Credentials needed to run SSH tests")
	}

	st, err := ConfigureStorage(
		t.Context(),
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

type failedOpenClient struct {
	SFTPClient
	err error
}

func (c failedOpenClient) Open(string) (*sftp.File, error) { return nil, c.err }

func TestReadObjectOpenErrors(t *testing.T) {
	for _, cause := range []error{os.ErrPermission, io.ErrUnexpectedEOF, os.ErrNotExist} {
		once := new(sync.Once)
		once.Do(func() {})
		folder := NewFolder(&SFTPLazy{client: failedOpenClient{err: cause}, once: once}, "archive")
		_, err := folder.ReadObject(t.Context(), "listed_sentinel.json")
		var missing storage.ObjectNotFoundError
		if cause == os.ErrNotExist {
			require.ErrorAs(t, err, &missing)
			continue
		}
		require.ErrorIs(t, err, cause)
		require.False(t, errors.As(err, &missing))
	}
}
