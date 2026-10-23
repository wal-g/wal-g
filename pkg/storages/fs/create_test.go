package fs

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestCreateObjectExclusive(t *testing.T) {
	root := t.TempDir()
	const name = "reservations/backup.json"
	results := make(chan error, 32)
	var wg sync.WaitGroup
	for range cap(results) {
		wg.Go(func() {
			// Independent handles, as on different backup hosts.
			results <- NewFolder(root, "").CreateObject(t.Context(), name, []byte("owner"))
		})
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, storage.ErrObjectAlreadyExists)
		}
	}
	require.Equal(t, 1, winners)
	folder := NewFolder(root, "")
	require.ErrorIs(t, folder.CreateObject(t.Context(), name, []byte("replacement")), storage.ErrObjectAlreadyExists)
	data, err := os.ReadFile(folder.GetFilePath(name))
	require.NoError(t, err)
	require.Equal(t, "owner", string(data))
}

func TestCreateObjectCanceledAndEmpty(t *testing.T) {
	folder := NewFolder(t.TempDir(), "")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.True(t, errors.Is(folder.CreateObject(ctx, "canceled", nil), context.Canceled))
	exists, err := folder.Exists(t.Context(), "canceled")
	require.NoError(t, err)
	require.False(t, exists)
	// An empty reservation left by a crashed writer still excludes later writers.
	require.NoError(t, folder.CreateObject(t.Context(), "empty", nil))
	require.ErrorIs(t, folder.CreateObject(t.Context(), "empty", []byte("new")), storage.ErrObjectAlreadyExists)
}
