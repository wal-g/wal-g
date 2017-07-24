package walg

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

/**
 *  TarWalker walks files provided by the IN directory and creates compressed tar members
 *  labeled as `part_00i.tar.lzo`. For files and directories that are skipped, please
 *  consult EXCLUDE in structs.go.
 */
func (bundle *Bundle) TarWalker(path string, info os.FileInfo, err error) error {
	if err != nil {
		panic(err)
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
			panic(err)
		}
	} else {
		oldTB := bundle.Tb
		oldTB.CloseTar()

		fmt.Println("------------------------------------------NEW------------------------------------------")
		bundle.NewTarBall()
		err = HandleTar(bundle, path, info)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			panic(err)
		}
	}
	return nil
}

/**
 *  Creates underlying Writer and handles one file. Does not follow symlinks. If file/directory
 *  is in EXCLUDE, will not be included in the final file.
 *  ISSUES: follow symlink, write too long error occurs sporadically
 */
func HandleTar(bundle TarBundle, path string, info os.FileInfo) error {
	tarBall := bundle.GetTarBall()
	fileName := info.Name()
	_, ok := EXCLUDE[info.Name()]
	tarBall.SetUp()
	tarWriter := tarBall.Tw()

	if tarBall.Nop() && !ok {
		fmt.Println("------------------------------------------", fileName)
		return nil
	}

	var hdr *tar.Header
	var err error

	if !ok {
		fmt.Println("------------------------------------------", fileName)
		hdr, err = tar.FileInfoHeader(info, fileName)
		if err != nil {
			panic(err)
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println("NAME:", hdr.Name)

		err = tarWriter.WriteHeader(hdr)
		if err != nil {
			panic(err)
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			lim := &io.LimitedReader{
				R: f,
				N: int64(hdr.Size),
			}

			fmt.Println("Writing tar ...")

			_, err = io.Copy(tarWriter, lim)
			if err != nil {
				panic(err)
			}

			tarBall.SetSize(hdr.Size)
			f.Close()
		}
	} else if ok && info.Mode().IsDir() {
		fmt.Println("------------------------------------------", fileName)
		hdr, err = tar.FileInfoHeader(info, fileName)
		if err != nil {
			panic(err)
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println("NAME:", hdr.Name)

		err = tarWriter.WriteHeader(hdr)
		if err != nil {
			panic(err)
		}
		fmt.Println("RUNNING:", tarBall.Size())
		return filepath.SkipDir
	}

	fmt.Println("RUNNING:", tarBall.Size())
	return nil
}
