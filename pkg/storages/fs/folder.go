package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/contextio"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const dirDefaultMode = 0755

// Folder represents folder on the file system
// TODO: Unit tests
type Folder struct {
	rootPath string
	subPath  string
}

func NewFolder(rootPath string, subPath string) *Folder {
	// Trim leading slash because all subPaths are relative.
	subPath = strings.TrimPrefix(subPath, "/")
	return &Folder{rootPath, subPath}
}

func (folder *Folder) GetPath() string {
	return folder.subPath
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	dirPath := path.Join(folder.rootPath, folder.subPath)
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to list FS folder %q: %w", dirPath, err)
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			// I do not use GetSubfolder() intentially
			subPath := path.Join(folder.subPath, fileInfo.Name()) + "/"
			subFolders = append(subFolders, NewFolder(folder.rootPath, subPath))
		} else {
			info, _ := fileInfo.Info()
			objects = append(objects, storage.NewLocalObject(fileInfo.Name(), info.ModTime(), info.Size()))
		}
	}
	return
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, fileName := range objectRelativePaths {
		err := os.RemoveAll(folder.GetFilePath(fileName))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("unable to delete file %q: %w", fileName, err)
		}
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	_, err := os.Stat(folder.GetFilePath(objectRelativePath))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("unable to get file stats %v: %w", objectRelativePath, err)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	sf := NewFolder(folder.rootPath, path.Join(folder.subPath, subFolderRelativePath))
	_ = sf.EnsureExists()

	// This is something unusual when we cannot be sure that our subfolder exists in FS
	// But we do not have to guarantee folder persistence, but any subsequent calls will fail
	// Just like in all other Storage Folders
	return sf
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	filePath := folder.GetFilePath(objectRelativePath)
	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil, storage.NewObjectNotFoundError(filePath)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to read file %v: %w", filePath, err)
	}
	return file, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.subPath)
	filePath := folder.GetFilePath(name)
	file, err := OpenFileWithDir(filePath)
	if err != nil {
		return fmt.Errorf("unable to open file %q: %w", filePath, err)
	}
	_, err = io.Copy(file, content)
	if err != nil {
		closerErr := file.Close()
		if closerErr != nil {
			tracelog.InfoLogger.Println("Error during closing failed upload ", closerErr)
		}
		return fmt.Errorf("unable to copy data to file %q: %w", filePath, err)
	}
	err = file.Close()
	if err != nil {
		return fmt.Errorf("unable to close file %q: %w", filePath, err)
	}
	return nil
}

func (folder *Folder) PutObjectWithContext(ctx context.Context, name string, content io.Reader) error {
	ctxReader := contextio.NewReader(ctx, content)
	return folder.PutObject(name, ctxReader)
}

func (folder *Folder) CopyObject(srcPath string, dstPath string) error {
	src := path.Join(folder.rootPath, srcPath)
	srcStat, err := os.Stat(src)
	if errors.Is(err, os.ErrNotExist) {
		return storage.NewObjectNotFoundError(srcPath)
	}
	if err != nil {
		return fmt.Errorf("unable to get file stats %q: %w", srcPath, err)
	}
	if !srcStat.Mode().IsRegular() {
		return fmt.Errorf("unable to copy file: %s is not a regular file", srcPath)
	}
	file, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("unable to open file to copy %q: %w", srcPath, err)
	}
	err = folder.PutObject(dstPath, file)
	if err != nil {
		return fmt.Errorf("unable to copy: %w", err)
	}
	return nil
}

func OpenFileWithDir(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if os.IsNotExist(err) {
		parentDir := path.Dir(filePath)
		err = os.MkdirAll(parentDir, dirDefaultMode)
		if err != nil {
			return nil, fmt.Errorf("unable to create a directory %q: %w", parentDir, err)
		}
		file, err = os.Create(filePath)
	}
	return file, err
}

func (folder *Folder) GetFilePath(objectRelativePath string) string {
	return path.Join(folder.rootPath, folder.subPath, objectRelativePath)
}

func (folder *Folder) EnsureExists() error {
	dirname := path.Join(folder.rootPath, folder.subPath)
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dirname, dirDefaultMode)
		if err != nil {
			return fmt.Errorf("unable to recursively create a directory %q: %w", dirname, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to get a directory stats %q: %w", dirname, err)
	}
	return nil
}

func (folder *Folder) Validate() error {
	return nil
}
