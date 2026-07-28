package pin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesPinnerPinTreePreservesRelativePathsAndKeepsFilesReadable(t *testing.T) {
	sourceDir := t.TempDir()
	pinDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "nested"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "root.rdb"), []byte("root"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "nested", "part.dat"), []byte("part"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "nested", "ignore.tmp"), []byte("tmp"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "nested", "ignore.lock"), []byte("lock"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "nested", "ignore.pid"), []byte("pid"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "nested", "ignore.part"), []byte("part"), 0o600))

	pinner := NewFilesPinner(pinDir)
	pinnedPaths, err := pinner.PinTree(sourceDir)
	require.NoError(t, err)
	require.Len(t, pinnedPaths, 2)
	defer pinner.Unpin()

	rootPinned := filepath.Join(pinDir, "root.rdb")
	nestedPinned := filepath.Join(pinDir, "nested", "part.dat")
	assert.FileExists(t, rootPinned)
	assert.FileExists(t, nestedPinned)
	assert.NoFileExists(t, filepath.Join(pinDir, "nested", "ignore.tmp"))
	assert.NoFileExists(t, filepath.Join(pinDir, "nested", "ignore.lock"))
	assert.NoFileExists(t, filepath.Join(pinDir, "nested", "ignore.pid"))
	assert.NoFileExists(t, filepath.Join(pinDir, "nested", "ignore.part"))

	require.NoError(t, os.RemoveAll(sourceDir))
	contents, err := os.ReadFile(nestedPinned)
	require.NoError(t, err)
	assert.Equal(t, []byte("part"), contents)
}

func TestValidateSameFilesystemRejectsMissingPinFolder(t *testing.T) {
	sourceDir := t.TempDir()
	err := ValidateSameFilesystem(sourceDir, filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pin folder")
}

func TestFilesPinnerUnpinRemovesPinnedFiles(t *testing.T) {
	sourceDir := t.TempDir()
	pinDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "file")
	require.NoError(t, os.WriteFile(sourceFile, []byte("data"), 0o600))

	pinner := NewFilesPinner(pinDir)
	_, err := pinner.Pin([]string{sourceFile})
	require.NoError(t, err)
	pinner.Unpin()

	assert.NoFileExists(t, filepath.Join(pinDir, "file"))
}
