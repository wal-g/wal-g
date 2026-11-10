package internal

import (
	"archive/tar"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTarFileInfoHeaderStoresSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "link")
	// A relative target without separators, so the test behaves the same on Windows,
	// where absolute targets come back from os.Readlink with backslashes.
	require.NoError(t, os.Symlink("target-file", link))
	info, err := os.Lstat(link)
	require.NoError(t, err)

	bundle := &Bundle{Directory: dir}
	header, err := bundle.createTarFileInfoHeader(link, info)
	require.NoError(t, err)

	assert.Equal(t, byte(tar.TypeSymlink), header.Typeflag)
	assert.Equal(t, "target-file", header.Linkname)
}

func TestCreateTarFileInfoHeaderRegularFileHasNoLinkname(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	require.NoError(t, os.WriteFile(file, []byte("data"), 0600))
	info, err := os.Lstat(file)
	require.NoError(t, err)

	bundle := &Bundle{Directory: dir}
	header, err := bundle.createTarFileInfoHeader(file, info)
	require.NoError(t, err)

	assert.Equal(t, byte(tar.TypeReg), header.Typeflag)
	assert.Empty(t, header.Linkname)
}
