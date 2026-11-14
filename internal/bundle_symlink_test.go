package internal

import (
	"archive/tar"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTarFileInfoHeaderStoresSymlinkTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
	}{
		{name: "relative", target: "target-file"},
		// An absolute target, like Stolon's postgresql.auto.conf -> /dev/null.
		{name: "absolute", target: "/dev/null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if filepath.IsAbs(tc.target) && runtime.GOOS == "windows" {
				t.Skip("os.Readlink returns absolute targets with backslashes on Windows")
			}
			dir := t.TempDir()
			link := filepath.Join(dir, "link")
			require.NoError(t, os.Symlink(tc.target, link))
			info, err := os.Lstat(link)
			require.NoError(t, err)

			bundle := &Bundle{Directory: dir}
			header, err := bundle.createTarFileInfoHeader(link, info)
			require.NoError(t, err)

			assert.Equal(t, byte(tar.TypeSymlink), header.Typeflag)
			assert.Equal(t, tc.target, header.Linkname)
		})
	}
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
