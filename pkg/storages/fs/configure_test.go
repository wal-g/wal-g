package fs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestConfigureStorageStripsWaleFileURLPrefix(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := ConfigureStorage(waleFileURL+tmpDir, nil)

	require.NoError(t, err)
	require.NotNil(t, st)

	err = st.RootFolder().PutObject("prefix-check.txt", strings.NewReader(""))
	require.NoError(t, err)

	_, statErr := os.Stat(filepath.Join(tmpDir, "prefix-check.txt"))
	assert.NoError(t, statErr)
}

func TestConfigureStorageReturnsWrappedErrorForMissingRoot(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing-root")

	st, err := ConfigureStorage(missingDir, nil)

	assert.Nil(t, st)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.ErrorContains(t, err, "create FS storage")
	assert.ErrorContains(t, err, "FS storage root directory doesn't exist or is inaccessible")
}

func TestConfigureStorageAppliesRootWraps(t *testing.T) {
	tmpDir := t.TempDir()
	wrapCalled := false

	wrap := func(prev storage.Folder) storage.Folder {
		wrapCalled = true
		return prev.GetSubFolder("wrapped")
	}

	st, err := ConfigureStorage(tmpDir, nil, wrap)
	require.NoError(t, err)
	require.True(t, wrapCalled)
	require.NotNil(t, st)

	err = st.RootFolder().PutObject("wrapped-check.txt", strings.NewReader(""))
	require.NoError(t, err)

	_, statErr := os.Stat(filepath.Join(tmpDir, "wrapped", "wrapped-check.txt"))
	assert.NoError(t, statErr)
}
