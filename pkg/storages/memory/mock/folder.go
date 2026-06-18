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
	ListFolderMock    func(ctx context.Context) (objects []storage.Object, subFolders []storage.Folder, err error)
	DeleteObjectsMock func(ctx context.Context, objectRelativePaths []storage.Object) error
	ExistsMock        func(ctx context.Context, objectRelativePath string) (bool, error)
	GetSubFolderMock  func(subFolderRelativePath string) storage.Folder
	ReadObjectMock    func(ctx context.Context, objectRelativePath string) (io.ReadCloser, error)
	PutObjectMock     func(ctx context.Context, name string, content io.Reader) error
	CopyObjectMock    func(ctx context.Context, srcPath string, dstPath string) error
	ValidateMock      func() error
}

func NewFolder(memFolder *memory.Folder) *Folder {
	return &Folder{
		MemFolder: memFolder,
	}
}

func (f *Folder) Exists(ctx context.Context, objectRelativePath string) (bool, error) {
	if f.ExistsMock != nil {
		return f.ExistsMock(ctx, objectRelativePath)
	}
	return f.MemFolder.Exists(ctx, objectRelativePath)
}

func (f *Folder) GetPath() string {
	if f.GetPathMock != nil {
		return f.GetPathMock()
	}
	return f.MemFolder.GetPath()
}

func (f *Folder) ListFolder(ctx context.Context) (objects []storage.Object, subFolders []storage.Folder, err error) {
	if f.ListFolderMock != nil {
		return f.ListFolderMock(ctx)
	}
	return f.MemFolder.ListFolder(ctx)
}

func (f *Folder) DeleteObjects(ctx context.Context, objectRelativePaths []storage.Object) error {
	if f.DeleteObjectsMock != nil {
		return f.DeleteObjectsMock(ctx, objectRelativePaths)
	}
	return f.MemFolder.DeleteObjects(ctx, objectRelativePaths)
}

func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	if f.GetSubFolderMock != nil {
		return f.GetSubFolderMock(subFolderRelativePath)
	}
	return f.MemFolder.GetSubFolder(subFolderRelativePath)
}

func (f *Folder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	if f.ReadObjectMock != nil {
		return f.ReadObjectMock(ctx, objectRelativePath)
	}
	return f.MemFolder.ReadObject(ctx, objectRelativePath)
}

func (f *Folder) PutObject(ctx context.Context, name string, content io.Reader) error {
	if f.PutObjectMock != nil {
		return f.PutObjectMock(ctx, name, content)
	}
	return f.MemFolder.PutObject(ctx, name, content)
}

func (f *Folder) CopyObject(ctx context.Context, srcPath string, dstPath string) error {
	if f.CopyObjectMock != nil {
		return f.CopyObjectMock(ctx, srcPath, dstPath)
	}
	return f.MemFolder.CopyObject(ctx, srcPath, dstPath)
}

func (f *Folder) Validate(ctx context.Context) error {
	if f.ValidateMock != nil {
		return f.ValidateMock()
	}
	return f.MemFolder.Validate(ctx)
}

// NOT IMPLEMENTED
func (f *Folder) SetVersioningEnabled(_ context.Context, enable bool) {}

// NOT IMPLEMENTED
func (f *Folder) GetVersioningEnabled(_ context.Context) bool {
	return false
}
