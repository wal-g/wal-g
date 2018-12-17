package internal

import (
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"io/ioutil"
	"os"
	"path"
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

func ConfigureFSFolder(path string) (StorageFolder, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err // Not exists or is inaccessible
	}
	return NewFSFolder(path, ""), nil
}

func (folder *FSFolder) GetPath() string {
	return folder.subpath
}

func (folder *FSFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	files, err := ioutil.ReadDir(path.Join(folder.rootPath, folder.subpath))
	if err != nil {
		return nil, nil, err
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
			return err
		}
	}
	return nil
}

func (folder *FSFolder) Exists(objectRelativePath string) (bool, error) {
	_, err := os.Stat(folder.GetFilePath(objectRelativePath));
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func (folder *FSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	sf := FSFolder{folder.rootPath, path.Join(folder.subpath, subFolderRelativePath)}
	err := sf.EnsureExists()
	if err != nil {
		// This is something unusual when we cannot be sure that our subfolder exists in FS
		// The program should not proceed
		tracelog.ErrorLogger.FatalError(err)
	}
	return &sf
}

func (folder *FSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	return os.Open(folder.GetFilePath(objectRelativePath))
}

func (folder *FSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.subpath)
	filePath := folder.GetFilePath(name)
	file, err := OpenFileWithDir(filePath)
	if err != nil {
		return err
	}
	_, err = io.Copy(file, content)
	if err != nil {
		return err
	}
	return file.Close()
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
