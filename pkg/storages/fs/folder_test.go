package fs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestFSFolder(t *testing.T) {
	tmpDir := setupTmpDir(t)

	defer os.RemoveAll(tmpDir)

	st, err := ConfigureStorage(t.Context(), tmpDir, nil)
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func TestDeleteObjectsRemovesEmptyDirs(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	st, err := ConfigureStorage(t.Context(), tmpDir, nil)
	assert.NoError(t, err)
	root := st.RootFolder()

	// Simulate a real backup layout:
	//   backup_001.json.lz4          — sentinel at root level
	//   backup_001/pg_data/global/pg_control
	//   backup_001/pg_data/base/1/1259
	err = root.PutObject(t.Context(), "backup_001.json.lz4", strings.NewReader("{}"))
	assert.NoError(t, err)
	err = root.PutObject(t.Context(), "backup_001/pg_data/global/pg_control", strings.NewReader("data"))
	assert.NoError(t, err)
	err = root.PutObject(t.Context(), "backup_001/pg_data/base/1/1259", strings.NewReader("data"))
	assert.NoError(t, err)

	// Delete sentinel + all data files, same as DeleteBackups does.
	err = root.DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("backup_001.json.lz4", time.Time{}, 0),
		storage.NewLocalObject("backup_001/pg_data/global/pg_control", time.Time{}, 0),
		storage.NewLocalObject("backup_001/pg_data/base/1/1259", time.Time{}, 0),
	})
	assert.NoError(t, err)

	// The backup directory itself and all nested dirs must be gone.
	for _, dir := range []string{
		"backup_001/pg_data/global",
		"backup_001/pg_data/base/1",
		"backup_001/pg_data/base",
		"backup_001/pg_data",
		"backup_001",
	} {
		_, statErr := os.Stat(filepath.Join(tmpDir, dir))
		assert.True(t, os.IsNotExist(statErr), "expected directory to be removed: %s", dir)
	}
}

func TestDeleteObjectsKeepsNonEmptyDirs(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	st, err := ConfigureStorage(t.Context(), tmpDir, nil)
	assert.NoError(t, err)
	root := st.RootFolder()

	// Two backups share a common parent layout but are independent.
	err = root.PutObject(t.Context(), "backup_001/pg_data/file_a", strings.NewReader("data"))
	assert.NoError(t, err)
	err = root.PutObject(t.Context(), "backup_002/pg_data/file_b", strings.NewReader("data"))
	assert.NoError(t, err)

	// Delete only backup_001's file.
	err = root.DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject("backup_001/pg_data/file_a", time.Time{}, 0),
	})
	assert.NoError(t, err)

	// backup_001 subtree must be gone.
	for _, dir := range []string{"backup_001/pg_data", "backup_001"} {
		_, statErr := os.Stat(filepath.Join(tmpDir, dir))
		assert.True(t, os.IsNotExist(statErr), "expected directory to be removed: %s", dir)
	}

	// backup_002 subtree must still be intact.
	_, statErr := os.Stat(filepath.Join(tmpDir, "backup_002/pg_data"))
	assert.NoError(t, statErr, "backup_002/pg_data should still exist")
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
