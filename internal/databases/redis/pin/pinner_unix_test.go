//go:build !windows

package pin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesPinnerKeepsUnlinkedFileReadableThroughHeldDescriptor(t *testing.T) {
	sourceDir := t.TempDir()
	pinDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "file")
	contents := []byte("data kept alive by the pinned descriptor")
	require.NoError(t, os.WriteFile(sourceFile, contents, 0o600))

	pinner := NewFilesPinner(pinDir)
	pinnedPaths, err := pinner.Pin([]string{sourceFile})
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(pinDir, "file")}, pinnedPaths)
	require.Len(t, pinner.openFiles, 1)
	heldFile := pinner.openFiles[0]

	// Simulate both the source tree and the pin mirror being unlinked. At this
	// point, the held descriptor is the only reference keeping the inode alive.
	require.NoError(t, os.Remove(sourceFile))
	require.NoError(t, os.Remove(pinnedPaths[0]))
	assert.NoFileExists(t, sourceFile)
	assert.NoFileExists(t, pinnedPaths[0])

	read := make([]byte, len(contents))
	n, err := heldFile.ReadAt(read, 0)
	require.NoError(t, err)
	assert.Equal(t, len(contents), n)
	assert.Equal(t, contents, read)

	pinner.Unpin()
	_, err = heldFile.ReadAt(make([]byte, 1), 0)
	assert.ErrorIs(t, err, os.ErrClosed)
	assert.Empty(t, pinner.openFiles)
	assert.Empty(t, pinner.pinnedPaths)
}
