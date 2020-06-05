package internal

import (
	"archive/tar"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileNew(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[fileInfo.Name]
	isIncremented := haveFileDescription && fileDescription.IsIncremented
	// todo: clearer catchup backup detection logic
	isCatchup := tarInterpreter.createNewIncrementalFiles

	if localFileInfo, _ := getLocalFileInfo(targetPath); localFileInfo != nil {
		isPageFile := isPagedFile(localFileInfo, targetPath)
		return unwrapToExistFile(fileReader, fileInfo, targetPath, isPageFile, isIncremented, isCatchup)
	}
	return unwrapToNewFile(fileReader, fileInfo, targetPath, isIncremented)
}

// unwrap the file from tar to existing local file
func unwrapToExistFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string,
	isPageFile, isIncremented, isCatchup bool) error {
	localFile, err := os.OpenFile(targetPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	if isIncremented {
		err := WritePagesFromIncrement(fileReader, localFile, isCatchup)
		return errors.Wrapf(err, "Interpret: failed to write increment to file '%s'", targetPath)
	}
	if isCatchup {
		err := clearLocalFile(localFile)
		if err != nil {
			return err
		}
		return writeLocalFile(fileReader, fileInfo, localFile)
	}
	if isPageFile {
		err := RestoreMissingPages(fileReader, localFile)
		return errors.Wrapf(err, "Interpret: failed to restore pages for file '%s'", targetPath)
	}
	// skip the non-page file because newer version is already on the disk
	return nil
}

// unwrap file from tar to new local file
func unwrapToNewFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string, isIncremented bool) error {
	localFile, err := createLocalFile(targetPath, fileInfo)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	if isIncremented {
		err := CreateFileFromIncrement(fileReader, localFile)
		return errors.Wrapf(err, "Interpret: failed to create file from increment '%s'", targetPath)
	}
	return writeLocalFile(fileReader, fileInfo, localFile)
}

func getLocalFileInfo(filename string) (fileInfo os.FileInfo, err error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("Requested file is directory. Aborting.")
	}
	return info, nil
}

func createLocalFile(targetPath string, fileInfo *tar.Header) (*os.File, error) {
	err := PrepareDirs(fileInfo.Name, targetPath)
	if err != nil {
		return nil, errors.Wrap(err, "Interpret: failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}
	return file, nil
}

func clearLocalFile(file *os.File) error {
	err := file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = file.Seek(0,0)
	return err
}

// Write file from backup to local file
func writeLocalFile(fileReader io.Reader, fileInfo *tar.Header, localFile *os.File) error {
	_, err := io.Copy(localFile, fileReader)
	if err != nil {
		err1 := localFile.Close()
		if err1 != nil {
			tracelog.ErrorLogger.Printf("Interpret: failed to close localFile '%s' because of error: %v",
				localFile.Name(), err1)
		}
		err1 = os.Remove(localFile.Name())
		if err1 != nil {
			tracelog.ErrorLogger.Fatalf("Interpret: failed to remove localFile '%s' because of error: %v",
				localFile.Name(), err1)
		}
		return errors.Wrap(err, "Interpret: copy failed")
	}

	mode := os.FileMode(fileInfo.Mode)
	if err = os.Chmod(localFile.Name(), mode); err != nil {
		return errors.Wrap(err, "Interpret: chmod failed")
	}

	err = localFile.Sync()
	return errors.Wrap(err, "Interpret: fsync failed")
}
