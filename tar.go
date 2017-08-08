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
	Interpret(r io.Reader, hdr *tar.Header) error
}

// FileTarInterpreter extracts input to disk.
type FileTarInterpreter struct {
	NewDir string
}

// Interpet extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	targetPath := path.Join(ti.NewDir, cur.Name)
	switch cur.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		var f *os.File

		f, err := os.Create(targetPath)
		dne := os.IsNotExist(err)
		if dne {
			base := filepath.Base(cur.Name)
			dir := strings.TrimSuffix(targetPath, base)
			err := os.MkdirAll(dir, 0755)
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
			return errors.Wrapf(err, "Interpret: failed to create symlink", targetPath)
		}
	}

	fmt.Println(cur.Name)
	return nil
}
