package walg

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

/**
 *  TarWalker walks files provided by the passed in directory and creates compressed tar members
 *  labeled as `part_00i.tar.lzo`. To see which files and directories are skipped, please
 *  consult EXCLUDE in 'structs.go'. Excluded directories will be created but their contents will
 *  not be included in the tar bundle.
 */
func (bundle *Bundle) TarWalker(path string, info os.FileInfo, err error) error {
	if err != nil {
		return errors.Wrap(err, "TarWalker: walk failed")
	}

	if info.Name() == "pg_control" {
		bundle.Sen = &Sentinel{info, path}
	} else if bundle.Tb.Size() <= bundle.MinSize {
		fmt.Println("---SIZE:", bundle.MinSize)
		err = HandleTar(bundle, path, info)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			return errors.Wrap(err, "TarWalker: handle tar failed")
		}
	} else {
		oldTB := bundle.Tb
		err := oldTB.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalker: failed to close tarball")
		}

		fmt.Println("------------------------------------------NEW------------------------------------------")
		bundle.NewTarBall()
		err = HandleTar(bundle, path, info)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			return errors.Wrap(err, "TarWalker: handle tar failed")
		}
	}
	return nil
}

/**
 *  Creates underlying tar writer and handles one given file. Does not follow symlinks. If file
 *  is in EXCLUDE, will not be included in the final file. EXCLUDED directories are created
 *  but their contents are not written to local disk.
 */
func HandleTar(bundle TarBundle, path string, info os.FileInfo) error {
	tarBall := bundle.GetTarBall()
	fileName := info.Name()
	_, ok := EXCLUDE[info.Name()]
	tarBall.SetUp()
	tarWriter := tarBall.Tw()

	if !ok {
		fmt.Println("------------------------------------------", fileName)
		hdr, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: could not grab header info")
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println("NAME:", hdr.Name)

		err = tarWriter.WriteHeader(hdr)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to write header")
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return errors.Wrapf(err, "HandleTar: failed to open file %s\n", path)
			}
			lim := &io.LimitedReader{
				R: f,
				N: int64(hdr.Size),
			}

			fmt.Println("Writing tar ...")

			_, err = io.Copy(tarWriter, lim)
			if err != nil {
				return errors.Wrap(err, "HandleTar: copy failed")
			}

			tarBall.SetSize(hdr.Size)
			f.Close()
		}
	} else if ok && info.Mode().IsDir() {
		fmt.Println("------------------------------------------", fileName)
		hdr, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to grab header info")
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println("NAME:", hdr.Name)

		err = tarWriter.WriteHeader(hdr)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to write header")
		}
		fmt.Println("RUNNING:", tarBall.Size())
		return filepath.SkipDir
	}

	fmt.Println("RUNNING:", tarBall.Size())
	return nil
}
