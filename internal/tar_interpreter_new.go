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
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileNew(fileReader io.Reader, header *tar.Header, targetPath string) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[header.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", header.Name)
			return nil
		}
	}
	fileUnwrapper := getFileUnwrapper(tarInterpreter, header, targetPath)
	if localFileInfo, _ := getLocalFileInfo(targetPath); localFileInfo != nil {
		return unwrapToExistFile(fileReader, header, targetPath, fileUnwrapper)
	}
	return unwrapToNewFile(fileReader, header, targetPath, fileUnwrapper)
}

// get file unwrapper for file depending on backup type
func getFileUnwrapper(tarInterpreter *FileTarInterpreter, header *tar.Header, targetPath string) IBackupFileUnwrapper {
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[header.Name]
	isIncremented := haveFileDescription && fileDescription.IsIncremented
	var isPageFile bool
	if localFileInfo, _ := getLocalFileInfo(targetPath); localFileInfo != nil {
		isPageFile = isPagedFile(localFileInfo, targetPath)
	}
	options := &BackupFileOptions{isIncremented: isIncremented, isPageFile: isPageFile}

	// todo: clearer catchup backup detection logic
	isCatchup := tarInterpreter.createNewIncrementalFiles
	if isCatchup {
		return NewFileUnwrapper(CatchupBackupFileUnwrapper, options)
	}
	return NewFileUnwrapper(DefaultBackupFileUnwrapper, options)
}

// unwrap the file from tar to existing local file
func unwrapToExistFile(fileReader io.Reader, header *tar.Header, targetPath string, unwrapper IBackupFileUnwrapper) error {
	localFile, err := os.OpenFile(targetPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	return unwrapper.UnwrapExistingFile(fileReader, header, localFile)
}

// unwrap file from tar to new local file
func unwrapToNewFile(fileReader io.Reader, header *tar.Header, targetPath string, unwrapper IBackupFileUnwrapper) error {
	newFile, err := createLocalFile(targetPath, header.Name)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(newFile, "")
	return unwrapper.UnwrapNewFile(fileReader, header, newFile)
}

// get file info by file path
func getLocalFileInfo(targetPath string) (fileInfo os.FileInfo, err error) {
	info, err := os.Stat(targetPath)
	if os.IsNotExist(err) {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("Requested file is directory. Aborting.")
	}
	return info, nil
}

// create new local file on disk
func createLocalFile(targetPath, name string) (*os.File, error) {
	err := PrepareDirs(name, targetPath)
	if err != nil {
		return nil, errors.Wrap(err, "Interpret: failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}
	return file, nil
}
