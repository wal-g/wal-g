package sh

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/contextio"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO: Unit tests
type Folder struct {
	sftpLazy *SFTPLazy
	path     string
}

func NewFolder(sftpLazy *SFTPLazy, path string) *Folder {
	return &Folder{
		sftpLazy: sftpLazy,
		path:     path,
	}
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	client, err := folder.sftpLazy.Client()
	if err != nil {
		return nil, nil, err
	}

	filesInfo, err := client.ReadDir(folder.path)

	if os.IsNotExist(err) {
		// The folder does not exist, it means there are no objects in it
		tracelog.DebugLogger.Println("\tnonexistent skipped " + folder.path + ": " + err.Error())
		return nil, nil, nil
	}

	if err != nil {
		return nil, nil, fmt.Errorf("read SFTP folder %q: %w", folder.path, err)
	}

	for _, fileInfo := range filesInfo {
		if fileInfo.IsDir() {
			subFolder := NewFolder(folder.sftpLazy, client.Join(folder.path, fileInfo.Name()))
			subFolders = append(subFolders, subFolder)
			// Folder is not object, just skip it
			continue
		}

		object := storage.NewLocalObject(
			fileInfo.Name(),
			fileInfo.ModTime(),
			fileInfo.Size(),
		)
		objects = append(objects, object)
	}

	return objects, subFolders, err
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	client, err := folder.sftpLazy.Client()
	if err != nil {
		return err
	}

	for _, relativePath := range objectRelativePaths {
		objPath := client.Join(folder.path, relativePath)

		stat, err := client.Stat(objPath)
		if errors.Is(err, os.ErrNotExist) {
			// Don't throw error if the file doesn't exist, to follow the storage.Folder contract
			continue
		}
		if err != nil {
			return fmt.Errorf("get stats of object %q via SFTP: %w", objPath, err)
		}

		// Do not try to remove directory. It may be not empty. TODO: remove if empty
		if stat.IsDir() {
			continue
		}

		err = client.Remove(objPath)
		if errors.Is(err, os.ErrNotExist) {
			// Don't throw error if the file doesn't exist, to follow the storage.Folder contract
			continue
		}
		if err != nil {
			return fmt.Errorf("delete object %q via SFTP: %w", objPath, err)
		}
	}

	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	client, err := folder.sftpLazy.Client()
	if err != nil {
		return false, err
	}

	objPath := filepath.Join(folder.path, objectRelativePath)
	_, err = client.Stat(objPath)

	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("check file %q existence via SFTP: %w", objPath, err)
	}

	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.sftpLazy, path.Join(folder.path, subFolderRelativePath))
}

const defaultBufferSize = 64 * 1024 * 1024

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	client, err := folder.sftpLazy.Client()
	if err != nil {
		return nil, err
	}

	objPath := path.Join(folder.path, objectRelativePath)
	file, err := client.Open(objPath)
	if err != nil {
		return nil, storage.NewObjectNotFoundError(objPath)
	}

	return struct {
		io.Reader
		io.Closer
	}{bufio.NewReaderSize(file, defaultBufferSize), file}, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	client, err := folder.sftpLazy.Client()
	if err != nil {
		return err
	}

	absolutePath := filepath.Join(folder.path, name)

	dirPath := filepath.Dir(absolutePath)
	err = client.MkdirAll(dirPath)
	if err != nil {
		return fmt.Errorf("create directory %q via SFTP: %w", dirPath, err)
	}

	file, err := client.Create(absolutePath)
	if err != nil {
		return fmt.Errorf("create file %q via SFTP: %w", absolutePath, err)
	}

	_, err = io.Copy(file, content)
	if err != nil {
		closerErr := file.Close()
		if closerErr != nil {
			tracelog.InfoLogger.Println("Error during closing failed upload ", closerErr)
		}
		return fmt.Errorf("write data to file %q via SFTP: %w", absolutePath, err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("close file %q opened via SFTP: %w", absolutePath, err)
	}
	return nil
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	ctxReader := contextio.NewReader(ctx, content)
	return folder.PutObject(name, ctxReader)
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	if exists, err := folder.Exists(srcPath); !exists {
		if err == nil {
			return storage.NewObjectNotFoundError(srcPath)
		}
		return fmt.Errorf("copy via SFTP: check if source file %q exists: %w", srcPath, err)
	}
	file, err := folder.ReadObject(srcPath)
	if err != nil {
		return fmt.Errorf("copy via SFTP: read source file %q: %w", srcPath, err)
	}
	err = folder.PutObject(dstPath, file)
	if err != nil {
		return fmt.Errorf("copy via SFTP: write destination file %q: %w", dstPath, err)
	}
	return nil
}

func (folder *Folder) Validate() error {
	return nil
}
