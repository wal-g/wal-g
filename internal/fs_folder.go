package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

const dirDefaultMode = 0755

// FSFolder represents folder of file system
type FSFolder struct {
	rootPath string
	subpath  string
}

func NewFSFolder(rootPath string, subPath string) *FSFolder {
	return &FSFolder{rootPath, subPath}
}

type FSFolderError struct {
	error
}

func NewFSFolderError(err error, format string, args ...interface{}) FSFolderError {
	return FSFolderError{errors.Wrapf(err, format, args...)}
}

func (err FSFolderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func ConfigureFSFolder(path string) (StorageFolder, error) {
	path = strings.TrimPrefix(path, "file://localhost") // WAL-E backward compatibility
	if _, err := os.Stat(path); err != nil {
		return nil, NewFSFolderError(err, "Folder not exists or is inaccessible")
	}
	return NewFSFolder(path, ""), nil
}

func (folder *FSFolder) GetPath() string {
	return folder.subpath
}

func (folder *FSFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	files, err := ioutil.ReadDir(path.Join(folder.rootPath, folder.subpath))
	if err != nil {
		return nil, nil, NewFSFolderError(err, "Unable to read folder")
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			// I do not use GetSubfolder() intentially
			subPath := path.Join(folder.subpath, fileInfo.Name()) + "/"
			subFolders = append(subFolders, NewFSFolder(folder.rootPath, subPath))
		} else {
			objects = append(objects, &FileStorageObject{fileInfo})
		}
	}
	return
}

func (folder *FSFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, fileName := range objectRelativePaths {
		err := os.RemoveAll(folder.GetFilePath(fileName))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return NewFSFolderError(err, "Unable to delete object %v", fileName)
		}
	}
	return nil
}

func (folder *FSFolder) Exists(objectRelativePath string) (bool, error) {
	_, err := os.Stat(folder.GetFilePath(objectRelativePath))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, NewFSFolderError(err, "Unable to stat object %v", objectRelativePath)
	}
	return true, nil

}
func (folder *FSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	sf := FSFolder{folder.rootPath, path.Join(folder.subpath, subFolderRelativePath)}
	_ = sf.EnsureExists()

	// This is something unusual when we cannot be sure that our subfolder exists in FS
	// But we do not have to guarantee folder persistence, but any subsequent calls will fail
	// Just like in all other Storage Folders
	return &sf
}

func (folder *FSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	filePath := folder.GetFilePath(objectRelativePath)
	file, err := os.Open(filePath)
	if os.IsNotExist(err) {
		return nil, NewObjectNotFoundError(filePath)
	}
	if err != nil {
		return nil, NewFSFolderError(err, "Unable to read object %v", filePath)
	}
	return file, nil
}

func (folder *FSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.subpath)
	filePath := folder.GetFilePath(name)
	file, err := OpenFileWithDir(filePath)
	if err != nil {
		return NewFSFolderError(err, "Unable to open file %v", filePath)
	}
	_, err = io.Copy(file, content)
	if err != nil {
		return NewFSFolderError(err, "Unable to copy data to %v", filePath)
	}
	err = file.Close()
	if err != nil {
		return NewFSFolderError(err, "Unable to close %v", filePath)
	}
	return nil
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

func (folder *FSFolder) GetFilePath(objectRelativePath string) string {
	return path.Join(folder.rootPath, folder.subpath, objectRelativePath)
}

func (folder *FSFolder) EnsureExists() error {
	dirname := path.Join(folder.rootPath, folder.subpath)
	_, err := os.Stat(dirname)
	if os.IsNotExist(err) {
		return os.MkdirAll(dirname, dirDefaultMode)

	}
	return err
}
