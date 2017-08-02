package walg

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type TarInterpreter interface {
	Interpret(r io.Reader, hdr *tar.Header) error
}

type FileTarInterpreter struct {
	NewDir string
}

type BufferTarInterpreter struct {
	Out []byte
}

/**
 *  Handles in memory tar formats. Mostly for testing purposes.
 */
func (ti *BufferTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	//defer TimeTrack(time.Now(), "BUFFER INTERPRET")
	//Assumes only regular files
	out, err := ioutil.ReadAll(tr)
	if err != nil {
		return errors.Wrap(err, "Interpret: ReadAll failed")
	}
	ti.Out = out
	return nil
}

/**
 *  Extracts a tar file to local disk and creates needed directories.
 *  TODO: test symlinks
 */
func (ti *FileTarInterpreter) Interpret(tr io.Reader, cur *tar.Header) error {
	targetPath := path.Join(ti.NewDir, cur.Name)
	switch cur.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		var f *os.File
		var err error

		f, err = os.Create(targetPath)
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
				return errors.Wrap(err, "Interpret: failed to create new file")
			}
		}
		if err != nil && !dne {
			return errors.Wrap(err, "Interpret: failed to create new file")
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
			return errors.Wrap(err, "Interpret: failed to close file")
		}
	case tar.TypeDir:
		err := os.MkdirAll(targetPath, 0755)
		if err != nil {
			return errors.Wrap(err, "Interpret: failed to create all directories")
		}
		if err = os.Chmod(targetPath, os.FileMode(cur.Mode)); err != nil {
			return errors.Wrap(err, "Interpret: chmod failed")
		}
	case tar.TypeLink:
		if err := os.Link(cur.Name, targetPath); err != nil {
			return errors.Wrap(err, "Interpret: failed to create hardlink")
		}
	case tar.TypeSymlink:
		if err := os.Symlink(cur.Name, targetPath); err != nil {
			return errors.Wrap(err, "Interpret: failed to create symlink")
		}
	}

	fmt.Println(cur.Name)
	return nil
}
