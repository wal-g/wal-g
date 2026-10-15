package rclone

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// Live check against a real rclone remote. Skipped unless RCLONE_LIVE_TEST=1.
//
//	export RCLONE_LIVE_TEST=1
//	export RCLONE_REMOTE=dropbox
//	export WALG_RCLONE_PREFIX='dropbox://walg-smoke'
//	go test ./pkg/storages/rclone/ -run Live -count=1 -v
func TestLiveFolderRoundTrip(t *testing.T) {
	if os.Getenv("RCLONE_LIVE_TEST") != "1" {
		t.Skip("set RCLONE_LIVE_TEST=1 with RCLONE_REMOTE and WALG_RCLONE_PREFIX to run")
	}

	remote := os.Getenv("RCLONE_REMOTE")
	prefix := os.Getenv("WALG_RCLONE_PREFIX")
	require.NotEmpty(t, remote, "RCLONE_REMOTE is required for the live test")
	require.NotEmpty(t, prefix, "WALG_RCLONE_PREFIX is required for the live test")

	settings := map[string]string{
		"RCLONE_REMOTE": remote,
	}
	if p := os.Getenv("RCLONE_CONFIG_PATH"); p != "" {
		settings["RCLONE_CONFIG_PATH"] = p
	}
	if b := os.Getenv("RCLONE_BINARY_PATH"); b != "" {
		settings["RCLONE_BINARY_PATH"] = b
	} else if homeBin := path.Join(os.Getenv("HOME"), ".local", "bin", "rclone"); fileExists(homeBin) {
		settings["RCLONE_BINARY_PATH"] = homeBin
	}

	st, err := ConfigureStorage(context.Background(), prefix, settings)
	require.NoError(t, err)
	folder := st.RootFolder()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := fmt.Sprintf("live-%d.txt", time.Now().UnixNano())
	copyName := name + ".copy"
	payload := []byte("wal-g rclone live check\n")

	cleanup := func(names ...string) {
		objs := make([]storage.Object, 0, len(names))
		for _, n := range names {
			objs = append(objs, storage.NewLocalObject(n, time.Now(), int64(len(payload))))
		}
		_ = folder.DeleteObjects(context.Background(), objs)
	}
	cleaned := false
	t.Cleanup(func() {
		if !cleaned {
			cleanup(name, copyName)
		}
	})

	require.NoError(t, folder.PutObject(ctx, name, bytes.NewReader(payload)))

	exists, err := folder.Exists(ctx, name)
	require.NoError(t, err)
	assert.True(t, exists)

	info, err := folder.StatObject(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), info.GetSize())

	objects, _, err := folder.ListFolder(ctx)
	require.NoError(t, err)
	assert.True(t, containsObject(objects, name), "list should include %s", name)

	rc, err := folder.ReadObject(ctx, name)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, payload, got)

	require.NoError(t, folder.CopyObject(ctx, name, copyName))
	exists, err = folder.Exists(ctx, copyName)
	require.NoError(t, err)
	assert.True(t, exists)

	cleanup(name, copyName)
	cleaned = true
	exists, err = folder.Exists(ctx, name)
	require.NoError(t, err)
	assert.False(t, exists)
}

func containsObject(objects []storage.Object, name string) bool {
	for _, obj := range objects {
		if obj.GetName() == name || strings.HasSuffix(obj.GetName(), name) {
			return true
		}
	}
	return false
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
