package internal_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

func TestLimitedFolderCreateObject(t *testing.T) {
	folder := internal.NewLimitedFolder(fs.NewFolder(t.TempDir(), ""), rate.NewLimiter(rate.Inf, 1)).GetSubFolder("cluster")
	require.NoError(t, storage.CreateObject(t.Context(), folder, "reservation", []byte("owner")))
	require.ErrorIs(t, storage.CreateObject(t.Context(), folder, "reservation", []byte("other")), storage.ErrObjectAlreadyExists)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, storage.CreateObject(ctx, folder, "canceled", []byte("owner")), context.Canceled)
	exists, err := folder.Exists(t.Context(), "canceled")
	require.NoError(t, err)
	require.False(t, exists)
}
