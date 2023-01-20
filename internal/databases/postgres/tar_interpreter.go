package postgres

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type IncrementalTarInterpreter interface {
	internal.TarInterpreter
	GetUnwrapResult() *UnwrapResult
}

// FileTarInterpreter extracts input to disk.
type FileTarInterpreter struct {
	DBDataDirectory string
	Sentinel        BackupSentinelDto
	FilesMetadata   FilesMetadataDto
	FilesToUnwrap   map[string]bool
	UnwrapResult    *UnwrapResult

	createNewIncrementalFiles bool
}

func NewFileTarInterpreter(
	dbDataDirectory string, sentinel BackupSentinelDto, filesMetadata FilesMetadataDto,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool,
) *FileTarInterpreter {
	return &FileTarInterpreter{dbDataDirectory, sentinel, filesMetadata,
		filesToUnwrap, newUnwrapResult(), createNewIncrementalFiles}
}

func (tarInterpreter *FileTarInterpreter) GetUnwrapResult() *UnwrapResult {
	return tarInterpreter.UnwrapResult
}

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileOld(fileReader io.Reader,
	fileInfo *tar.Header,
	targetPath string,
	fsync bool) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}
	fileDescription, haveFileDescription := tarInterpreter.FilesMetadata.Files[fileInfo.Name]

	// If this file is incremental we use it's base version from incremental path
	if haveFileDescription && tarInterpreter.Sentinel.IsIncremental() && fileDescription.IsIncremented {
		err := ApplyFileIncrement(targetPath, fileReader, tarInterpreter.createNewIncrementalFiles, fsync)
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
	defer utility.LoggedClose(file, "")

	return utility.WriteLocalFile(fileReader, fileInfo, file, fsync)
}

// Interpret extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (tarInterpreter *FileTarInterpreter) Interpret(fileReader io.Reader, fileInfo *tar.Header) error {
	tracelog.DebugLogger.Println("Interpreting: ", fileInfo.Name)
	targetPath := path.Join(tarInterpreter.DBDataDirectory, fileInfo.Name)
	fsync := !viper.GetBool(internal.TarDisableFsyncSetting)
	switch fileInfo.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		// temporary switch to determine if new unwrap logic should be used
		if useNewUnwrapImplementation {
			return tarInterpreter.unwrapRegularFileNew(fileReader, fileInfo, targetPath, fsync)
		}
		return tarInterpreter.unwrapRegularFileOld(fileReader, fileInfo, targetPath, fsync)
	case tar.TypeDir:
		err := os.MkdirAll(targetPath, 0750)
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
	err := os.MkdirAll(dir, 0750)
	return err
}
