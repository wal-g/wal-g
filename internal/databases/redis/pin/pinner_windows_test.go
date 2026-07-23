//go:build windows

package pin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilesPinnerPreventsRemovalOfOpenPinnedFile(t *testing.T) {
	sourceDir := t.TempDir()
	pinDir := t.TempDir()
	sourceFile := filepath.Join(sourceDir, "file")
	require.NoError(t, os.WriteFile(sourceFile, []byte("pinned data"), 0o600))

	pinner := NewFilesPinner(pinDir)
	pinnedPaths, err := pinner.Pin([]string{sourceFile})
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(pinDir, "file")}, pinnedPaths)

	// Windows prevents removal while the pinner retains an open descriptor.
	require.Error(t, os.Remove(pinnedPaths[0]))

	pinner.Unpin()
	assert.NoFileExists(t, pinnedPaths[0])
	assert.FileExists(t, sourceFile)
}
