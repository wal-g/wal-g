package rclone

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// rcloneBinaryForTest returns the rclone binary to use, or skips the test when
// none is available: set RCLONE_BINARY_PATH or put rclone into PATH.
func rcloneBinaryForTest(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("RCLONE_BINARY_PATH"); p != "" {
		return p
	}
	p, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone binary not found: set RCLONE_BINARY_PATH or install rclone to run this test")
	}
	return p
}

// newLocalRcloneFolder builds a Folder backed by a throwaway rclone "alias"
// remote that points to a temp directory, so no real cloud storage is needed.
func newLocalRcloneFolder(t *testing.T) (storage.Folder, string) {
	t.Helper()
	binary := rcloneBinaryForTest(t)

	root := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "rclone.conf")
	require.NoError(t, os.WriteFile(configPath, []byte(fmt.Sprintf("[loc]\ntype = alias\nremote = %s\n", root)), 0o600))

	st, err := ConfigureStorage(context.Background(), "rclone://bucket/walg", map[string]string{
		remoteNameSetting:   "loc",
		configPathSetting:   configPath,
		rcloneBinarySetting: binary,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	return st.RootFolder(), filepath.Join(root, "bucket", "walg")
}

func readAll(t *testing.T, folder storage.Folder, name string) string {
	t.Helper()
	rc, err := folder.ReadObject(context.Background(), name)
	require.NoError(t, err)
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	return string(data)
}

func TestFolderIntegration_RoundTrip(t *testing.T) {
	ctx := context.Background()
	folder, onDisk := newLocalRcloneFolder(t)

	// Put + read back, including a nested path that does not exist yet.
	require.NoError(t, folder.PutObject(ctx, "dir/a.txt", strings.NewReader("hello")))
	assert.Equal(t, "hello", readAll(t, folder, "dir/a.txt"))

	// The object really landed under <root>/bucket/walg/dir/a.txt.
	data, err := os.ReadFile(filepath.Join(onDisk, "dir", "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))

	// Exists / StatObject.
	exists, err := folder.Exists(ctx, "dir/a.txt")
	require.NoError(t, err)
	assert.True(t, exists)

	obj, err := folder.StatObject(ctx, "dir/a.txt")
	require.NoError(t, err)
	assert.Equal(t, int64(5), obj.GetSize())

	exists, err = folder.Exists(ctx, "dir/missing.txt")
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = folder.StatObject(ctx, "dir/missing.txt")
	var notFound storage.ObjectNotFoundError
	assert.True(t, errors.As(err, &notFound), "expected ObjectNotFoundError, got %v", err)

	// ListFolder: root has sub-folder "dir"; "dir" has object a.txt.
	objects, subFolders, err := folder.ListFolder(ctx)
	require.NoError(t, err)
	assert.Empty(t, objects)
	require.Len(t, subFolders, 1)
	assert.Equal(t, "dir/", subFolders[0].GetPath())

	objects, _, err = folder.GetSubFolder("dir").ListFolder(ctx)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.Equal(t, "a.txt", objects[0].GetName())
	assert.Equal(t, int64(5), objects[0].GetSize())

	// Copy, then delete both.
	require.NoError(t, folder.CopyObject(ctx, "dir/a.txt", "dir/b.txt"))
	assert.Equal(t, "hello", readAll(t, folder, "dir/b.txt"))

	require.NoError(t, folder.DeleteObjects(ctx, []storage.Object{
		storage.NewLocalObject("dir/a.txt", obj.GetLastModified(), obj.GetSize()),
		storage.NewLocalObject("dir/b.txt", obj.GetLastModified(), obj.GetSize()),
	}))
	exists, err = folder.Exists(ctx, "dir/a.txt")
	require.NoError(t, err)
	assert.False(t, exists)

	// Deleting something that is already gone must not fail.
	require.NoError(t, folder.DeleteObjects(ctx, []storage.Object{
		storage.NewLocalObject("dir/a.txt", obj.GetLastModified(), 0),
	}))
}

func TestFolderIntegration_ListMissingFolderIsEmpty(t *testing.T) {
	ctx := context.Background()
	folder, _ := newLocalRcloneFolder(t)

	objects, subFolders, err := folder.GetSubFolder("nope").ListFolder(ctx)
	require.NoError(t, err)
	assert.Empty(t, objects)
	assert.Empty(t, subFolders)
}

// A remote defined only through the parent's environment (RCLONE_CONFIG_<NAME>_*)
// must keep working when RCLONE_CONFIG_PATH is set: the rclone child process has
// to inherit the parent's environment rather than receive only RCLONE_CONFIG.
func TestFolderIntegration_InheritsEnvironmentWithConfigPath(t *testing.T) {
	ctx := context.Background()
	binary := rcloneBinaryForTest(t)

	root := t.TempDir()
	t.Setenv("RCLONE_CONFIG_ENVLOC_TYPE", "alias")
	t.Setenv("RCLONE_CONFIG_ENVLOC_REMOTE", root)

	configPath := filepath.Join(t.TempDir(), "empty.conf")
	require.NoError(t, os.WriteFile(configPath, nil, 0o600))

	st, err := ConfigureStorage(ctx, "rclone://bucket/walg", map[string]string{
		remoteNameSetting:   "envloc",
		configPathSetting:   configPath,
		rcloneBinarySetting: binary,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	folder := st.RootFolder()
	require.NoError(t, folder.PutObject(ctx, "x.txt", strings.NewReader("hi")))
	assert.Equal(t, "hi", readAll(t, folder, "x.txt"))

	exists, err := folder.Exists(ctx, "x.txt")
	require.NoError(t, err)
	assert.True(t, exists)
}

// rclone prints a NOTICE to stderr when its config file does not exist (the usual case in containers
// with an environment-defined remote). It must not end up in the JSON that ListFolder parses.
func TestFolderIntegration_ListIgnoresRcloneDiagnostics(t *testing.T) {
	ctx := context.Background()
	binary := rcloneBinaryForTest(t)

	root := t.TempDir()
	t.Setenv("RCLONE_CONFIG", filepath.Join(t.TempDir(), "missing.conf"))
	t.Setenv("RCLONE_CONFIG_ENVLOC_TYPE", "alias")
	t.Setenv("RCLONE_CONFIG_ENVLOC_REMOTE", root)

	st, err := ConfigureStorage(ctx, "rclone://bucket/walg", map[string]string{
		remoteNameSetting:   "envloc",
		rcloneBinarySetting: binary,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	folder := st.RootFolder()
	require.NoError(t, folder.PutObject(ctx, "x.txt", strings.NewReader("hi")))

	objects, _, err := folder.ListFolder(ctx)
	require.NoError(t, err)
	require.Len(t, objects, 1)
	assert.Equal(t, "x.txt", objects[0].GetName())
}
