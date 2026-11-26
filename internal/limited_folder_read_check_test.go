package internal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

type readProbeFolder struct {
	storage.Folder
	probed *[]string
}

func (f *readProbeFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return &readProbeFolder{Folder: f.Folder.GetSubFolder(subFolderRelativePath), probed: f.probed}
}

func (f *readProbeFolder) ListFolder(context.Context) ([]storage.Object, []storage.Folder, error) {
	return nil, nil, errors.New("read check must not list the folder")
}

func (f *readProbeFolder) CheckReadAccess(context.Context) error {
	*f.probed = append(*f.probed, f.GetPath())
	return nil
}

func TestLimitedFolderForwardsCheckReadAccess(t *testing.T) {
	var probed []string
	inner := &readProbeFolder{Folder: memory.NewFolder("root/", memory.NewKVS()), probed: &probed}
	root := internal.NewLimitedFolder(inner, rate.NewLimiter(rate.Inf, 1))

	require.NoError(t, storage.CheckReadAccess(t.Context(), root))
	require.NoError(t, storage.CheckReadAccess(t.Context(), root.GetSubFolder("prefix")))
	require.Equal(t, []string{"root/", "root/prefix/"}, probed)
}
