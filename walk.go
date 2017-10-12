package walg

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	//"golang.org/x/crypto/openpgp"
)

// ZeroReader generates a slice of zeroes. Used to pad
// tar in cases where length of file changes.
type ZeroReader struct{}

func (z *ZeroReader) Read(p []byte) (int, error) {
	zeroes := make([]byte, len(p))
	n := copy(p, zeroes)
	return n, nil

}

// TarWalker walks files provided by the passed in directory
// and creates compressed tar members labeled as `part_00i.tar.lzo`.
//
// To see which files and directories are Skipped, please consult
// 'structs.go'. Excluded directories will be created but their
// contents will not be included in the tar bundle.
func (bundle *Bundle) TarWalker(path string, info os.FileInfo, err error) error {
	if err != nil {
		return errors.Wrap(err, "TarWalker: walk failed")
	}

	if info.Name() == "pg_control" {
		bundle.Sen = &Sentinel{info, path}
	} else if bundle.Tb.Size() <= bundle.MinSize {
		err = HandleTar(bundle, path, info, &bundle.Crypter)
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

		bundle.NewTarBall()

		err = HandleTar(bundle, path, info, &bundle.Crypter)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			return errors.Wrap(err, "TarWalker: handle tar failed")
		}
	}
	return nil
}

// HandleTar creates underlying tar writer and handles one given file.
// Does not follow symlinks. If file is in EXCLUDE, will not be included
// in the final tarball. EXCLUDED directories are created
// but their contents are not written to local disk.
func HandleTar(bundle TarBundle, path string, info os.FileInfo, crypter Crypter) error {
	tarBall := bundle.GetTarBall()
	fileName := info.Name()
	_, ok := EXCLUDE[info.Name()]
	tarBall.SetUp(crypter)
	tarWriter := tarBall.Tw()

	if !ok {
		hdr, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: could not grab header info")
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println(hdr.Name)

		if info.Mode().IsRegular() {
			baseFiles := bundle.GetIncrementBaseFiles()
			bf, ok := baseFiles[hdr.Name]

			// It is important to take MTime before ReadDatabaseFile()
			time := info.ModTime()

			// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
			// For details see
			// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

			if ok && (time == bf.MTime) {
				// File was not changed since previous backup

				fmt.Println("Skiped by modification type")
				tarBall.GetFiles()[hdr.Name] = BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: time}

			} else {
				// !ok means file was not observed previously
				f, isPaged, size, err := ReadDatabaseFile(path, bundle.GetIncrementBaseLsn(), !ok)
				if err != nil {
					return errors.Wrapf(err, "HandleTar: failed to open file '%s'\n", path)
				}

				hdr.Size = size

				tarBall.GetFiles()[hdr.Name] = BackupFileDescription{IsSkipped: false, IsIncremented: isPaged, MTime: time}

				err = tarWriter.WriteHeader(hdr)
				if err != nil {
					return errors.Wrap(err, "HandleTar: failed to write header")
				}
				lim := &io.LimitedReader{
					R: io.MultiReader(f, &ZeroReader{}),
					N: int64(hdr.Size),
				}

				size, err = io.Copy(tarWriter, lim)
				if err != nil {
					return errors.Wrap(err, "HandleTar: copy failed")
				}

				tarBall.SetSize(hdr.Size)
				f.Close()
			}
		}
	} else if ok && info.Mode().IsDir() {
		hdr, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to grab header info")
		}

		hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
		fmt.Println(hdr.Name)

		err = tarWriter.WriteHeader(hdr)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to write header")
		}
		return filepath.SkipDir
	}

	return nil
}
