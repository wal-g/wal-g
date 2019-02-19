package internal

import (
	"archive/tar"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
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
}

func NewFileTarInterpreter(dbDataDirectory string, sentinel BackupSentinelDto, filesToUnwrap map[string]bool) *FileTarInterpreter {
	return &FileTarInterpreter{dbDataDirectory, sentinel, filesToUnwrap}
}

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[fileInfo.Name]

	// If this file is incremental we use it's base version from incremental path
	if haveFileDescription && tarInterpreter.Sentinel.isIncremental() && fileDescription.IsIncremented {
		err := ApplyFileIncrement(targetPath, fileReader)
		return errors.Wrapf(err, "Interpret: failed to apply increment for '%s'", targetPath)
	}
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}
	err := prepareDirs(fileInfo.Name, targetPath)
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
	defer file.Close()

	mode := os.FileMode(fileInfo.Mode)
	if err = os.Chmod(file.Name(), mode); err != nil {
		return errors.Wrap(err, "Interpret: chmod failed")
	}

	err = file.Sync()
	return errors.Wrap(err, "Interpret: fsync failed")
}

// TODO : unit tests
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

// Make sure all dirs exist
func prepareDirs(fileName string, targetPath string) error {
	base := filepath.Base(fileName)
	dir := strings.TrimSuffix(targetPath, base)
	err := os.MkdirAll(dir, 0755)
	return err
}
