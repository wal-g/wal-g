package internal

import (
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const dirDefaultMode = 0755

type FileStorageObject struct {
	os.FileInfo
}

func (fso FileStorageObject) GetName() string {
	return fso.Name()
}

func (fso FileStorageObject) GetLastModified() time.Time {
	return fso.ModTime()
}

// FSFolder represents folder of file system
type FSFolder struct {
	rootPath string
	subpath  string
}

func NewFSFolder(path string) (StorageFolder, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err // Not exists or is inaccessible
	}
	return &FSFolder{path, ""}, nil
}

func (f *FSFolder) GetPath() string {
	return f.subpath
}

func (f *FSFolder) ListFolder() (objects []StorageObject, subFolders []StorageFolder, err error) {
	files, err := ioutil.ReadDir(path.Join(f.rootPath, f.subpath))
	if err != nil {
		return nil, nil, err
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			// I do not use GetSubfolder() intentially
			subPath := path.Join(f.subpath, fileInfo.Name())
			subFolders = append(subFolders, &FSFolder{f.rootPath, subPath})
		} else {
			objects = append(objects, &FileStorageObject{fileInfo})
		}
	}
	return
}

func (f *FSFolder) DeleteObjects(objectRelativePaths []string) error {
	for _, fileName := range objectRelativePaths {
		err := os.RemoveAll(f.GetFilePath(fileName))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *FSFolder) Exists(objectRelativePath string) (bool, error) {
	_, err := os.Stat(f.GetFilePath(objectRelativePath));
	return !os.IsNotExist(err), err
}

func (f *FSFolder) GetSubFolder(subFolderRelativePath string) StorageFolder {
	sf := FSFolder{f.rootPath, path.Join(f.subpath, subFolderRelativePath)}
	sf.EnsureExists()
	return &sf
}

func (f *FSFolder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	return os.Open(f.GetFilePath(objectRelativePath))
}

func (f *FSFolder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v on into \n", name, f.subpath)
	filePath := f.GetFilePath(name)
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

func (f *FSFolder) GetFilePath(objectRelativePath string) string {
	return path.Join(f.rootPath, f.subpath, objectRelativePath)
}

func (f *FSFolder) EnsureExists() {
	dirname := path.Join(f.rootPath, f.subpath)
	if _, err := os.Stat(dirname); os.IsNotExist(err) {
		err := os.MkdirAll(dirname, dirDefaultMode)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
	}
}
