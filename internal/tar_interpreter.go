package internal

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// TarInterpreter behaves differently
// for different file types.
type TarInterpreter interface {
	Interpret(reader io.Reader, header *tar.Header) error
}

// FileTarInterpreter extracts input to disk.
type FileTarInterpreter struct {
	DBDataDirectory string
	Sentinel        BackupSentinelDto
	FilesToUnwrap   map[string]bool

	createNewIncrementalFiles bool
}

func NewFileTarInterpreter(
	dbDataDirectory string, sentinel BackupSentinelDto, filesToUnwrap map[string]bool, createNewIncrementalFiles bool,
) *FileTarInterpreter {
	return &FileTarInterpreter{dbDataDirectory, sentinel, filesToUnwrap, createNewIncrementalFiles}
}

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
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

// Interpret extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (tarInterpreter *FileTarInterpreter) Interpret(fileReader io.Reader, fileInfo *tar.Header) error {
	tracelog.DebugLogger.Println("Interpreting: ", fileInfo.Name)
	targetPath := path.Join(tarInterpreter.DBDataDirectory, fileInfo.Name)
	switch fileInfo.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		return tarInterpreter.unwrapRegularFile(fileReader, fileInfo, targetPath)
	case tar.TypeDir:
		err := os.MkdirAll(targetPath, 0755)
		if err != nil {
			return errors.Wrapf(err, "Interpret: failed to create all directories in %s", targetPath)
		}
		if err = os.Chmod(targetPath, os.FileMode(fileInfo.Mode)); err != nil {
			return errors.Wrap(err, "Interpret: chmod failed")
		}
	case tar.TypeLink:
		if err := os.Link(fileInfo.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create hardlink %s", targetPath)
		}
	case tar.TypeSymlink:
		if err := os.Symlink(fileInfo.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create symlink %s", targetPath)
		}
	}
	return nil
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

// PrepareDirs makes sure all dirs exist
func PrepareDirs(fileName string, targetPath string) error {
	if fileName == targetPath {
		return nil // because it runs in the local directory
	}
	base := filepath.Base(fileName)
	dir := strings.TrimSuffix(targetPath, base)
	err := os.MkdirAll(dir, 0755)
	return err
}
