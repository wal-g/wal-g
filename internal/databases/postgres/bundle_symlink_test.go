package postgres

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTarHeaderStoresSymlinkTarget(t *testing.T) {
	linkPath := filepath.Join(t.TempDir(), "postgresql.auto.conf")
	require.NoError(t, os.Symlink("link_target", linkPath))
	info, err := os.Lstat(linkPath)
	require.NoError(t, err)

	header, err := newTarHeader(linkPath, info)
	require.NoError(t, err)
	assert.Equal(t, byte(tar.TypeSymlink), header.Typeflag)
	assert.Equal(t, "link_target", header.Linkname)
}

func TestNewTarHeaderRegularFileHasNoLinkname(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "postgresql.conf")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0600))
	info, err := os.Lstat(filePath)
	require.NoError(t, err)

	header, err := newTarHeader(filePath, info)
	require.NoError(t, err)
	assert.Equal(t, byte(tar.TypeReg), header.Typeflag)
	assert.Empty(t, header.Linkname)
}
