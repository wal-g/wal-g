package mock

import (
	"context"
	"io"

	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type Folder struct {
	MemFolder *memory.Folder

	GetPathMock       func() string
	ListFolderMock    func() (objects []storage.Object, subFolders []storage.Folder, err error)
	DeleteObjectsMock func(objectRelativePaths []string) error
	ExistsMock        func(objectRelativePath string) (bool, error)
	GetSubFolderMock  func(subFolderRelativePath string) storage.Folder
	ReadObjectMock    func(objectRelativePath string) (io.ReadCloser, error)
	PutObjectMock     func(ctx context.Context, name string, content io.Reader) error
	CopyObjectMock    func(srcPath string, dstPath string) error
}

func NewFolder(memFolder *memory.Folder) *Folder {
	return &Folder{
		MemFolder: memFolder,
	}
}

func (f *Folder) Exists(objectRelativePath string) (bool, error) {
	if f.ExistsMock != nil {
		return f.ExistsMock(objectRelativePath)
	}
	return f.MemFolder.Exists(objectRelativePath)
}

func (f *Folder) GetPath() string {
	if f.GetPathMock != nil {
		return f.GetPathMock()
	}
	return f.MemFolder.GetPath()
}

func (f *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	if f.ListFolderMock != nil {
		return f.ListFolderMock()
	}
	return f.MemFolder.ListFolder()
}

func (f *Folder) DeleteObjects(objectRelativePaths []string) error {
	if f.DeleteObjectsMock != nil {
		return f.DeleteObjectsMock(objectRelativePaths)
	}
	return f.MemFolder.DeleteObjects(objectRelativePaths)
}

func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	if f.GetSubFolderMock != nil {
		return f.GetSubFolderMock(subFolderRelativePath)
	}
	return f.MemFolder.GetSubFolder(subFolderRelativePath)
}

func (f *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	if f.ReadObjectMock != nil {
		return f.ReadObjectMock(objectRelativePath)
	}
	return f.MemFolder.ReadObject(objectRelativePath)
}

func (f *Folder) PutObject(name string, content io.Reader) error {
	if f.PutObjectMock != nil {
		return f.PutObjectMock(context.Background(), name, content)
	}
	return f.MemFolder.PutObject(name, content)
}

func (f *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	if f.PutObjectMock != nil {
		return f.PutObjectMock(ctx, name, content)
	}
	return f.MemFolder.PutObjectWithContext(ctx, name, content)
}

func (f *Folder) CopyObject(srcPath string, dstPath string) error {
	if f.CopyObjectMock != nil {
		return f.CopyObjectMock(srcPath, dstPath)
	}
	return f.MemFolder.CopyObject(srcPath, dstPath)
}
