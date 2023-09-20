package fs

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
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

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "FS", format, args...)
}

// Folder represents folder of file system
type Folder struct {
	rootPath string
	subpath  string
}

func NewFolder(rootPath string, subPath string) *Folder {
	subPath = strings.TrimPrefix(subPath, "/")
	return &Folder{rootPath, subPath}
}

func NewFolderError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "GCS", format, args...)
}

func ConfigureFolder(path string, settings map[string]string) (storage.HashableFolder, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, NewError(err, "Folder not exists or is inaccessible")
	}
	return NewFolder(path, ""), nil
}

func (folder *Folder) GetPath() string {
	return folder.subpath
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	files, err := os.ReadDir(path.Join(folder.rootPath, folder.subpath))
	if err != nil {
		return nil, nil, NewError(err, "Unable to read folder")
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			// I do not use GetSubfolder() intentially
			subPath := path.Join(folder.subpath, fileInfo.Name()) + "/"
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
			return NewError(err, "Unable to delete object %v", fileName)
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
		return false, NewError(err, "Unable to stat object %v", objectRelativePath)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	sf := NewFolder(folder.rootPath, path.Join(folder.subpath, subFolderRelativePath))
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
		return nil, NewError(err, "Unable to read object %v", filePath)
	}
	return file, nil
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.subpath)
	filePath := folder.GetFilePath(name)
	file, err := OpenFileWithDir(filePath)
	if err != nil {
		return NewError(err, "Unable to open file %v", filePath)
	}
	_, err = io.Copy(file, content)
	if err != nil {
		closerErr := file.Close()
		if closerErr != nil {
			tracelog.InfoLogger.Println("Error during closing failed upload ", closerErr)
		}
		return NewError(err, "Unable to copy data to %v", filePath)
	}
	err = file.Close()
	if err != nil {
		return NewError(err, "Unable to close %v", filePath)
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
		return err
	}
	if !srcStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", srcPath)
	}
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	err = folder.PutObject(dstPath, file)
	return err
}

func OpenFileWithDir(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(path.Dir(filePath), dirDefaultMode)
		if err != nil {
			return nil, err
		}
		file, err = os.Create(filePath)
	}
	return file, err
}

func (folder *Folder) GetFilePath(objectRelativePath string) string {
	return path.Join(folder.rootPath, folder.subpath, objectRelativePath)
}

func (folder *Folder) EnsureExists() error {
	dirname := path.Join(folder.rootPath, folder.subpath)
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return os.MkdirAll(dirname, dirDefaultMode)
	}
	return err
}

func (folder *Folder) Hash() storage.Hash {
	hash := fnv.New64a()

	addToHash := func(data []byte) {
		_, err := hash.Write(data)
		if err != nil {
			// Writing to the hash function is always successful, so it mustn't be a problem that we panic here
			panic(err)
		}
	}

	addToHash([]byte("fs"))

	addToHash([]byte(folder.rootPath))

	addToHash([]byte(folder.subpath))

	userID := os.Getuid()
	userIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(userIDBytes, uint64(userID))
	addToHash(userIDBytes)

	return storage.Hash(hash.Sum64())
}
