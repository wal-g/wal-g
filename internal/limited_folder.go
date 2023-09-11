package internal

import (
	"context"
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/time/rate"
)

type LimitedFolder struct {
	storage.HashableFolder
	limiter *rate.Limiter
}

func NewLimitedFolder(folder storage.HashableFolder, limiter *rate.Limiter) *LimitedFolder {
	return &LimitedFolder{HashableFolder: folder, limiter: limiter}
}

func (lf *LimitedFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	folder := lf.HashableFolder.GetSubFolder(subFolderRelativePath).(storage.HashableFolder)
	return NewLimitedFolder(folder, lf.limiter)
}

func (lf *LimitedFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	readCloser, err := lf.HashableFolder.ReadObject(objectRelativePath)
	if err != nil {
		return nil, err
	}
	return ioextensions.ReadCascadeCloser{
		Reader: limiters.NewReader(context.Background(), readCloser, lf.limiter),
		Closer: readCloser,
	}, nil
}

func (lf *LimitedFolder) PutObject(name string, content io.Reader) error {
	return lf.PutObjectWithContext(context.Background(), name, content)
}

func (lf *LimitedFolder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	limitedReader := limiters.NewReader(ctx, content, lf.limiter)
	return lf.HashableFolder.PutObjectWithContext(ctx, name, limitedReader)
}
