package fs

import (
	"os"
	"path/filepath"
	"strings"
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

func TestDeleteObjectsRemovesEmptyDirs(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Create nested directory structure with a file
	testPath := "backups/base_123/data/file.txt"
	err := folder.PutObject(testPath, strings.NewReader("test content"))
	assert.NoError(t, err)

	// Verify the directory structure exists
	dirPath := filepath.Join(tmpDir, "backups/base_123/data")
	_, err = os.Stat(dirPath)
	assert.NoError(t, err, "directory should exist before deletion")

	// Delete the file
	err = folder.DeleteObjects([]string{testPath})
	assert.NoError(t, err)

	// Verify empty parent directories are removed
	_, err = os.Stat(dirPath)
	assert.True(t, os.IsNotExist(err), "empty data directory should be removed")

	_, err = os.Stat(filepath.Join(tmpDir, "backups/base_123"))
	assert.True(t, os.IsNotExist(err), "empty base_123 directory should be removed")

	_, err = os.Stat(filepath.Join(tmpDir, "backups"))
	assert.True(t, os.IsNotExist(err), "empty backups directory should be removed")

	// Root directory should still exist
	_, err = os.Stat(tmpDir)
	assert.NoError(t, err, "root directory should still exist")
}

func TestDeleteObjectsPreservesNonEmptyDirs(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Create two files in the same directory
	file1 := "backups/base_123/file1.txt"
	file2 := "backups/base_123/file2.txt"

	err := folder.PutObject(file1, strings.NewReader("content1"))
	assert.NoError(t, err)
	err = folder.PutObject(file2, strings.NewReader("content2"))
	assert.NoError(t, err)

	// Delete only one file
	err = folder.DeleteObjects([]string{file1})
	assert.NoError(t, err)

	// Directory should still exist because it contains file2
	dirPath := filepath.Join(tmpDir, "backups/base_123")
	_, err = os.Stat(dirPath)
	assert.NoError(t, err, "directory should still exist with remaining file")

	// file2 should still exist
	file2Path := filepath.Join(tmpDir, file2)
	_, err = os.Stat(file2Path)
	assert.NoError(t, err, "file2 should still exist")
}

func TestDeleteObjectsMultipleFiles(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Create multiple files in different directories
	files := []string{
		"backups/base_123/data/file1.txt",
		"backups/base_123/data/file2.txt",
		"backups/base_456/data/file3.txt",
	}

	for _, file := range files {
		err := folder.PutObject(file, strings.NewReader("content"))
		assert.NoError(t, err)
	}

	// Delete all files in base_123
	err := folder.DeleteObjects([]string{
		"backups/base_123/data/file1.txt",
		"backups/base_123/data/file2.txt",
	})
	assert.NoError(t, err)

	// base_123 should be completely removed
	_, err = os.Stat(filepath.Join(tmpDir, "backups/base_123"))
	assert.True(t, os.IsNotExist(err), "empty base_123 should be removed")

	// base_456 should still exist
	_, err = os.Stat(filepath.Join(tmpDir, "backups/base_456"))
	assert.NoError(t, err, "base_456 should still exist")

	// backups directory should still exist (contains base_456)
	_, err = os.Stat(filepath.Join(tmpDir, "backups"))
	assert.NoError(t, err, "backups should still exist")
}

func TestDeleteObjectsNonExistentFile(t *testing.T) {
	tmpDir := setupTmpDir(t)
	defer os.RemoveAll(tmpDir)

	folder := NewFolder(tmpDir, "")

	// Try to delete a file that doesn't exist - should not error
	err := folder.DeleteObjects([]string{"nonexistent/file.txt"})
	assert.NoError(t, err)
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
