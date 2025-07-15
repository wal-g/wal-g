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

func (lf *LimitedFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	readCloser, err := lf.Folder.ReadObject(objectRelativePath)
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
	return lf.Folder.PutObjectWithContext(ctx, name, limitedReader)
}
