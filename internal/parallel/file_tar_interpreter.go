package parallel

import (
	"archive/tar"
	"io"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type FileTarInterpreter struct {
	DirectoryToSave string
}

func NewFileTarInterpreter(directoryToSave string) internal.TarInterpreter {
	return &FileTarInterpreter{directoryToSave}
}

func (tarInterpreter *FileTarInterpreter) Interpret(reader io.Reader, fileInfo *tar.Header) error {
	tracelog.DebugLogger.Println("Interpreting: ", fileInfo.Name)
	targetPath := path.Join(tarInterpreter.DirectoryToSave, fileInfo.Name)
	fsync := !viper.GetBool(internal.TarDisableFsyncSetting)
	switch fileInfo.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		return tarInterpreter.interpretRegularFile(fsync, targetPath, fileInfo, reader)
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

func (tarInterpreter *FileTarInterpreter) interpretRegularFile(fsync bool, targetPath string, header *tar.Header, reader io.Reader) error {
	localFile, _, err := utility.GetLocalFile(targetPath, header)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	defer utility.LoggedSync(localFile, "", fsync)

	return utility.WriteLocalFile(reader, header, localFile, fsync)
}
