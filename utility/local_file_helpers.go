package utility

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

func GetLocalFile(targetPath string, header *tar.Header) (localFile *os.File, isNewFile bool, err error) {
	if localFileInfo, _ := GetLocalFileInfo(targetPath); localFileInfo != nil {
		localFile, err = os.OpenFile(targetPath, os.O_RDWR, 0666)
	} else {
		localFile, err = CreateLocalFile(targetPath, header.Name)
		isNewFile = true
	}
	return localFile, isNewFile, err
}

// get file info by file path
func GetLocalFileInfo(targetPath string) (fileInfo os.FileInfo, err error) {
	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("requested file is directory. Aborting")
	}
	return info, nil
}

// create new local file on disk
func CreateLocalFile(targetPath, name string) (*os.File, error) {
	err := PrepareDirs(name, targetPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}
	return file, nil
}

// creates parent dirs of the file
func CreateParentDirs(fileName string, targetPath string) error {
	if fileName == targetPath {
		return nil // because it runs in the local directory
	}
	base := filepath.Base(fileName)
	dir := strings.TrimSuffix(targetPath, base)
	err := os.MkdirAll(dir, 0755)
	return err
}

func WriteLocalFile(fileReader io.Reader, header *tar.Header, localFile *os.File, fsync bool) error {
	_, err := io.Copy(localFile, fileReader)
	if err != nil {
		err1 := os.Remove(localFile.Name())
		if err1 != nil {
			tracelog.ErrorLogger.Fatalf("failed to remove localFile '%s' because of error: %v",
				localFile.Name(), err1)
		}
		return errors.Wrap(err, "copy failed")
	}

	mode := os.FileMode(header.Mode)
	if err = localFile.Chmod(mode); err != nil {
		return errors.Wrap(err, "chmod failed")
	}

	if fsync {
		err = localFile.Sync()
		return errors.Wrap(err, "fsync failed")
	}

	return nil
}

func IsDirectoryEmpty(directoryPath string) (bool, error) {
	var isEmpty = true

	searchLambda := func(path string, info os.FileInfo, err error) error {
		if path != directoryPath {
			isEmpty = false
			tracelog.InfoLogger.Printf("found file '%s' in directory: '%s'\n", path, directoryPath)
		}
		return nil
	}
	err := filepath.Walk(directoryPath, searchLambda)
	return isEmpty, errors.Wrapf(err, "can't check, that directory: '%s' is empty", directoryPath)
}
