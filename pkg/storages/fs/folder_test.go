package fs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestFSFolder(t *testing.T) {
	tmpDir := setupTmpDir(t)

	defer os.RemoveAll(tmpDir)

	st, err := ConfigureStorage(tmpDir, nil)
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func setupTmpDir(t *testing.T) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	tmpDir, err := os.MkdirTemp(cwd, "data")
	if err != nil {
		t.Log(err)
	}
	err = os.Chmod(tmpDir, 0755)
	if err != nil {
		t.Log(err)
	}
	return tmpDir
}
