package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestFSFolder(t *testing.T) {
	tmpDir := setupTmpDir(t)

	defer os.RemoveAll(tmpDir)

	st, err := ConfigureStorage(tmpDir, nil)
	assert.NoError(t, err)

	storage.RunFolderTest(st.RootFolder(), t)
}

func TestDeleteObjects_RemovesEmptyDirectories(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Put a file in a nested subdirectory
	err := folder.PutObject("backup1/data/file.tar", &bytes.Buffer{})
	require.NoError(t, err)

	// Verify the file and directories exist
	_, err = os.Stat(filepath.Join(tmpDir, "backup1", "data", "file.tar"))
	require.NoError(t, err)

	// Delete the object
	err = folder.DeleteObjects([]storage.Object{storage.NewLocalObject("backup1/data/file.tar", time.Time{}, 0)})
	require.NoError(t, err)

	// Verify that empty subdirectories are cleaned up
	_, err = os.Stat(filepath.Join(tmpDir, "backup1", "data"))
	assert.True(t, os.IsNotExist(err), "empty 'data' directory should be removed")

	_, err = os.Stat(filepath.Join(tmpDir, "backup1"))
	assert.True(t, os.IsNotExist(err), "empty 'backup1' directory should be removed")

	// The root directory should still exist
	_, err = os.Stat(tmpDir)
	assert.NoError(t, err, "root directory should not be removed")
}

func TestDeleteObjects_KeepsNonEmptyDirectories(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Put two files in the same backup directory
	err := folder.PutObject("backup1/data/file1.tar", &bytes.Buffer{})
	require.NoError(t, err)
	err = folder.PutObject("backup1/data/file2.tar", &bytes.Buffer{})
	require.NoError(t, err)

	// Delete only one file
	err = folder.DeleteObjects([]storage.Object{storage.NewLocalObject("backup1/data/file1.tar", time.Time{}, 0)})
	require.NoError(t, err)

	// Directory should still exist because file2.tar is still there
	_, err = os.Stat(filepath.Join(tmpDir, "backup1", "data"))
	assert.NoError(t, err, "non-empty 'data' directory should not be removed")

	_, err = os.Stat(filepath.Join(tmpDir, "backup1", "data", "file2.tar"))
	assert.NoError(t, err, "remaining file should still exist")
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
