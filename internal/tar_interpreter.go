package internal

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

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
	mutex sync.Mutex
	DBDataDirectory string
	Sentinel        BackupSentinelDto
	FilesToUnwrap   map[string]bool
	CompletedFiles []string
	CreatedPageFiles map[string]int64
	WrittenIncrementFiles map[string]int64

	createNewIncrementalFiles bool
}

func NewFileTarInterpreter(
	dbDataDirectory string, sentinel BackupSentinelDto, filesToUnwrap map[string]bool, createNewIncrementalFiles bool,
) *FileTarInterpreter {
	return &FileTarInterpreter{sync.Mutex{},dbDataDirectory, sentinel,
		filesToUnwrap,make([]string,0), make(map[string]int64),
		make(map[string]int64),createNewIncrementalFiles}
}

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileOld(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[fileInfo.Name]

	// If this file is incremental we use it's base version from incremental path
	if haveFileDescription && tarInterpreter.Sentinel.IsIncremental() && fileDescription.IsIncremented {
		err := ApplyFileIncrement(targetPath, fileReader, tarInterpreter.createNewIncrementalFiles)
		return errors.Wrapf(err, "Interpret: failed to apply increment for '%s'", targetPath)
	}
	err := PrepareDirs(fileInfo.Name, targetPath)
	if err != nil {
		return errors.Wrap(err, "Interpret: failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}

	_, err = io.Copy(file, fileReader)
	if err != nil {
		err1 := file.Close()
		if err1 != nil {
			tracelog.ErrorLogger.Printf("Interpret: failed to close file '%s' because of error: %v", targetPath, err1)
		}
		err1 = os.Remove(targetPath)
		if err1 != nil {
			tracelog.ErrorLogger.Fatalf("Interpret: failed to remove file '%s' because of error: %v", targetPath, err1)
		}
		return errors.Wrap(err, "Interpret: copy failed")
	}
	defer utility.LoggedClose(file, "")

	mode := os.FileMode(fileInfo.Mode)
	if err = os.Chmod(file.Name(), mode); err != nil {
		return errors.Wrap(err, "Interpret: chmod failed")
	}

	err = file.Sync()
	return errors.Wrap(err, "Interpret: fsync failed")
}

// Interpret extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (tarInterpreter *FileTarInterpreter) Interpret(fileReader io.Reader, fileInfo *tar.Header) error {
	tracelog.DebugLogger.Println("Interpreting: ", fileInfo.Name)
	targetPath := path.Join(tarInterpreter.DBDataDirectory, fileInfo.Name)
	switch fileInfo.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		// temporary switch to determine if new unwrap logic should be used
		if useNewUnwrapImplementation {
			return tarInterpreter.unwrapRegularFileNew(fileReader, fileInfo, targetPath)
		}
		return tarInterpreter.unwrapRegularFileOld(fileReader, fileInfo, targetPath)
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
