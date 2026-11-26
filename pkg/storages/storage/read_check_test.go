package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type listCountingFolder struct {
	storage.Folder
	listCalls int
	listErr   error
}

func (f *listCountingFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	f.listCalls++
	if f.listErr != nil {
		return nil, nil, f.listErr
	}
	return f.Folder.ListFolder(ctx)
}

type probingFolder struct {
	listCountingFolder
	probeCalls int
	probeErr   error
}

func (f *probingFolder) CheckReadAccess(context.Context) error {
	f.probeCalls++
	return f.probeErr
}

func TestCheckReadAccessFallsBackToListFolder(t *testing.T) {
	folder := &listCountingFolder{Folder: memory.NewFolder("root/", memory.NewKVS())}
	require.NoError(t, storage.CheckReadAccess(t.Context(), folder))
	assert.Equal(t, 1, folder.listCalls)

	folder.listErr = errors.New("list failed")
	require.ErrorIs(t, storage.CheckReadAccess(t.Context(), folder), folder.listErr)
}

func TestCheckReadAccessUsesProbe(t *testing.T) {
	folder := &probingFolder{listCountingFolder: listCountingFolder{Folder: memory.NewFolder("root/", memory.NewKVS())}}
	require.NoError(t, storage.CheckReadAccess(t.Context(), folder))
	assert.Equal(t, 1, folder.probeCalls)
	assert.Zero(t, folder.listCalls)

	folder.probeErr = errors.New("probe failed")
	require.ErrorIs(t, storage.CheckReadAccess(t.Context(), folder), folder.probeErr)
	assert.Zero(t, folder.listCalls)
}
