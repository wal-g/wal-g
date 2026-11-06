package internal

import (
	"bytes"
	"context"
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

type LimitedFolder struct {
	storage.Folder
	limiter *rate.Limiter
}

func NewLimitedFolder(folder storage.Folder, limiter *rate.Limiter) *LimitedFolder {
	return &LimitedFolder{Folder: folder, limiter: limiter}
}

func (lf *LimitedFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	folder := lf.Folder.GetSubFolder(subFolderRelativePath)
	return NewLimitedFolder(folder, lf.limiter)
}

func (lf *LimitedFolder) ListObjectsWithPrefix(ctx context.Context, prefix string) ([]storage.Object, error) {
	return storage.ListObjectsWithPrefix(ctx, lf.Folder, prefix)
}

func (lf *LimitedFolder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	readCloser, err := lf.Folder.ReadObject(ctx, objectRelativePath)
	if err != nil {
		return nil, err
	}
	return ioextensions.ReadCascadeCloser{
		Reader: limiters.NewReader(ctx, readCloser, lf.limiter),
		Closer: readCloser,
	}, nil
}

func (lf *LimitedFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	limitedReader := limiters.NewReader(ctx, content, lf.limiter)
	return lf.Folder.PutObject(ctx, name, limitedReader)
}

func (lf *LimitedFolder) SupportsCreateObject() bool {
	return storage.SupportsCreateObject(lf.Folder)
}

func (lf *LimitedFolder) CreateObject(ctx context.Context, name string, content []byte) error {
	// Reservations are small buffered objects. Account for their bytes before
	// delegating the indivisible conditional write to the underlying storage.
	_, err := io.Copy(io.Discard, limiters.NewReader(ctx, bytes.NewReader(content), lf.limiter))
	if err != nil {
		return err
	}
	return storage.CreateObject(ctx, lf.Folder, name, content)
}

// SetShowAllVersions delegates the "show all versions" toggle to the underlying folder (if supported).
// This is used by storage tools (e.g. `wal-g st ls --all-versions`) and must work even when the folder is wrapped.
func (lf *LimitedFolder) SetShowAllVersions(show bool) {
	storage.SetShowAllVersions(lf.Folder, show)
}

func (lf *LimitedFolder) WithAllVersions() storage.Folder {
	return NewLimitedFolder(storage.WithAllVersions(lf.Folder), lf.limiter)
}

func (lf *LimitedFolder) WalkObjects(ctx context.Context, visit func(storage.Object) error) error {
	return storage.WalkObjects(ctx, lf.Folder, visit)
}
