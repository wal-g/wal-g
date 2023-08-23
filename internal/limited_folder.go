package internal

import (
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
		Reader: limiters.NewReader(readCloser, lf.limiter),
		Closer: readCloser,
	}, nil
}

func (lf *LimitedFolder) PutObject(name string, content io.Reader) error {
	limitedReader := limiters.NewReader(content, lf.limiter)
	return lf.HashableFolder.PutObject(name, limitedReader)
}
