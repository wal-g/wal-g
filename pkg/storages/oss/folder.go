package oss

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var _ storage.Folder = &Folder{}

type Folder struct {
	ossAPI *oss.Client
	bucket string
	path   string
	config *Config
}

func NewFolder(ossAPI *oss.Client, bucket string, path string, config *Config) *Folder {
	return &Folder{
		ossAPI: ossAPI,
		bucket: bucket,
		path:   path,
		config: config,
	}
}

// GetPath provides a relative path from the root of the storage. It must always end with '/'.
func (f *Folder) GetPath() string {
	if !strings.HasSuffix(f.path, "/") {
		f.path += "/"
	}
	return f.path
}

// ListFolder lists the folder and provides nested objects and folders. Objects must be with relative paths.
// If the folder doesn't exist, empty objects and subFolders must be returned without any error.
func (f *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	return nil, nil, fmt.Errorf("not implemented")
}

// DeleteObjects deletes objects from the storage if they exist.
func (f *Folder) DeleteObjects(objectRelativePaths []string) error {
	return fmt.Errorf("not implemented")
}

func (f *Folder) Exists(objectRelativePath string) (bool, error) {
	return false, fmt.Errorf("not implemented")
}

func (f *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return nil
}

// ReadObject reads an object from the folder. Must return ObjectNotFoundError in case the object doesn't exist.
func (f *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

// PutObject uploads a new object into the folder by a relative path. If an object with the same name already
// exists, it is overwritten. Please prefer using PutObjectWithContext.
func (f *Folder) PutObject(name string, content io.Reader) error {
	return fmt.Errorf("not implemented")
}

// PutObjectWithContext uploads a new object into the folder by a relative path. If an object with the same name
// already exists, it is overwritten. Operation can be terminated using Context.
func (f *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	return fmt.Errorf("not implemented")
}

// CopyObject copies an object from one place inside the folder to the other. Both paths must be relative. This is
// an error if the source object doesn't exist.
func (f *Folder) CopyObject(srcPath string, dstPath string) error {
	return fmt.Errorf("not implemented")
}

func (f *Folder) Validate() error {
	return fmt.Errorf("not implemented")
}

// Sets versioning setting. If versioning is disabled on server, sets it to disabled.
// Default versioning is set according to server setting.
func (f *Folder) SetVersioningEnabled(enable bool) {
	// Not implemented
}
