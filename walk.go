package walg

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

// ExcludedFilenames is a list of excluded members from the bundled backup.
var ExcludedFilenames = make(map[string]Empty)

func init() {
	filesToExclude := []string{
		"pg_log", "pg_xlog", "pg_wal",
		"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf",
		"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	}

	for _, filename := range filesToExclude {
		ExcludedFilenames[filename] = Empty{}
	}
}

// ZeroReader generates a slice of zeroes. Used to pad
// tar in cases where length of file changes.
type ZeroReader struct{}

func (z *ZeroReader) Read(p []byte) (int, error) {
	zeroes := make([]byte, len(p))
	n := copy(p, zeroes)
	return n, nil

}

// HandleTar creates underlying tar writer and handles one given file.
// Does not follow symlinks. If file is in ExcludedFilenames, will not be included
// in the final tarball. EXCLUDED directories are created
// but their contents are not written to local disk.
func HandleTar(bundle TarBundle, path string, info os.FileInfo, crypter Crypter) error {
	fileName := info.Name()
	_, excluded := ExcludedFilenames[info.Name()]

	tarBall := bundle.Deque()
	var parallelOpInProgress = false
	defer bundle.EnqueueBack(tarBall, &parallelOpInProgress)

	tarBall.SetUp(crypter)
	tarWriter := tarBall.TarWriter()

	if !excluded {
		fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: could not grab header info")
		}

		fileInfoHeader.Name = tarBall.GetFileRelPath(path)
		fmt.Println(fileInfoHeader.Name)

		if info.Mode().IsRegular() {
			baseFiles := bundle.GetIncrementBaseFiles()
			bf, wasInBase := baseFiles[fileInfoHeader.Name]

			// It is important to take MTime before ReadDatabaseFile()
			time := info.ModTime()

			// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
			// For details see
			// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

			if wasInBase && (time.Equal(bf.MTime)) {
				// File was not changed since previous backup

				fmt.Println("Skiped due to unchanged modification time")
				bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: time})

			} else {
				// !excluded means file was not observed previously
				worker := func() error {
					f, isPaged, size, err := ReadDatabaseFile(path, bundle.GetIncrementBaseLsn(), !wasInBase)
					if err != nil {
						return errors.Wrapf(err, "HandleTar: failed to open file '%s'\n", path)
					}

					fileInfoHeader.Size = size

					bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: false, IsIncremented: isPaged, MTime: time})

					err = tarWriter.WriteHeader(fileInfoHeader)
					if err != nil {
						return errors.Wrap(err, "HandleTar: failed to write header")
					}

					lim := &io.LimitedReader{
						R: io.MultiReader(f, &ZeroReader{}),
						N: int64(fileInfoHeader.Size),
					}

					size, err = io.Copy(tarWriter, lim)
					if err != nil {
						return errors.Wrap(err, "HandleTar: copy failed")
					}

					if size != fileInfoHeader.Size {
						return errors.Errorf("HandleTar: packed wrong numbers of bytes %d instead of %d", size, fileInfoHeader.Size)
					}

					tarBall.AddSize(fileInfoHeader.Size)
					f.Close()
					return nil
				}

				workerWrapper := func() {
					// TODO: Refactor this functional mess
					// And maybe do a better error handling
					workerError := worker()
					if workerError != nil {
						panic(workerError)
					}
					bundleError := bundle.CheckSizeAndEnqueueBack(tarBall)
					if bundleError != nil {
						panic(bundleError)
					}
				}

				parallelOpInProgress = true
				go workerWrapper()
			}
		} else {
			// It is not file
			err = tarWriter.WriteHeader(fileInfoHeader)
			if err != nil {
				return errors.Wrap(err, "HandleTar: failed to write header")
			}
		}
	} else if excluded && info.Mode().IsDir() {
		fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to grab header info")
		}

		fileInfoHeader.Name = tarBall.GetFileRelPath(path)
		fmt.Println(fileInfoHeader.Name)

		err = tarWriter.WriteHeader(fileInfoHeader)
		if err != nil {
			return errors.Wrap(err, "HandleTar: failed to write header")
		}
		return filepath.SkipDir
	}

	return nil
}
