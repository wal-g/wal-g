package walg

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
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
	NewDir             string
	Sentinel           S3TarBallSentinelDto
	IncrementalBaseDir string
}

// TODO : unit tests
// Interpret extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (tarInterpreter *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	fmt.Println(cur.Name)
	targetPath := path.Join(tarInterpreter.NewDir, cur.Name)
	// this path is only used for increment restoration
	incrementalPath := path.Join(tarInterpreter.IncrementalBaseDir, cur.Name)
	switch cur.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		fd, haveFd := tarInterpreter.Sentinel.Files[cur.Name]

		// If this file is incremental we use it's base version from incremental path
		if haveFd && tarInterpreter.Sentinel.isIncremental() && fd.IsIncremented {
			err := ApplyFileIncrement(incrementalPath, tr)
			if err != nil {
				return errors.Wrap(err, "Interpret: failed to apply increment for "+targetPath)
			}

			err = moveFileAndCreateDirs(incrementalPath, targetPath, cur.Name)
			if err != nil {
				return errors.Wrap(err, "Interpret: failed to move increment for "+targetPath)
			}
		} else {

			var f *os.File

			f, err := os.Create(targetPath)
			dne := os.IsNotExist(err)
			if dne {
				err := prepareDirs(cur.Name, targetPath)
				if err != nil {
					return errors.Wrap(err, "Interpret: failed to create all directories")
				}

				f, err = os.Create(targetPath)
				if err != nil {
					return errors.Wrapf(err, "Interpret: failed to create new file %s", targetPath)
				}
			}
			if err != nil && !dne {
				return errors.Wrapf(err, "Interpret: failed to create new file %s", targetPath)
			}

			_, err = io.Copy(f, tr)
			if err != nil {
				return errors.Wrap(err, "Interpret: copy failed")
			}

			mode := os.FileMode(cur.Mode)
			if err = os.Chmod(f.Name(), mode); err != nil {
				return errors.Wrap(err, "Interpret: chmod failed")
			}

			if err = f.Sync(); err != nil {
				return errors.Wrap(err, "Interpret: fsync failed")
			}

			if err = f.Close(); err != nil {
				return errors.Wrapf(err, "Interpret: failed to close file %s", targetPath)
			}
		}
	case tar.TypeDir:
		err := os.MkdirAll(targetPath, 0755)
		if err != nil {
			return errors.Wrapf(err, "Interpret: failed to create all directories in %s", targetPath)
		}
		if err = os.Chmod(targetPath, os.FileMode(cur.Mode)); err != nil {
			return errors.Wrap(err, "Interpret: chmod failed")
		}
	case tar.TypeLink:
		if err := os.Link(cur.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create hardlink %s", targetPath)
		}
	case tar.TypeSymlink:
		if err := os.Symlink(cur.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create symlink %s", targetPath)
		}
	}
	return nil
}

// TODO : unit tests
// moveFileAndCreateDirs moves file from incremental folder to target folder, creating necessary folders structure
func moveFileAndCreateDirs(incrementalPath string, targetPath string, fileName string) (err error) {
	err = os.Rename(incrementalPath, targetPath)
	if os.IsNotExist(err) {
		// this path is invoked if this is a first file in a dir
		err := prepareDirs(fileName, targetPath)
		if err != nil {
			return errors.Wrap(err, "moveFileAndCreateDirs: failed to create all directories")
		}
		err = os.Rename(incrementalPath, targetPath)
		if err != nil {
			return errors.Wrap(err, "moveFileAndCreateDirs: failed to rename incremented file "+targetPath)
		}
	} else if err != nil {
		return errors.Wrap(err, "moveFileAndCreateDirs: failed to rename incremented file "+targetPath)
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
