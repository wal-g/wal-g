package walg

import (
	"sync"
	"github.com/pkg/errors"
	"github.com/jackc/pgx"
	"log"
	"os"
	"fmt"
	"path/filepath"
	"archive/tar"
	"strings"
	"io"
)

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall will be at least
// MinSize bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	MinSize            int64
	Sentinel           *Sentinel
	TarBall            TarBall
	TarBallMaker       TarBallMaker
	Crypter            OpenPGPCrypter
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList

	tarballQueue     chan (TarBall)
	uploadQueue      chan (TarBall)
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          bool

	Files *sync.Map
}

func (bundle *Bundle) GetFiles() *sync.Map { return bundle.Files }

func (bundle *Bundle) StartQueue() {
	if bundle.started {
		panic("Trying to start already started Queue")
	}
	bundle.parallelTarballs = getMaxUploadDiskConcurrency()
	bundle.maxUploadQueue = getMaxUploadQueue()
	bundle.tarballQueue = make(chan (TarBall), bundle.parallelTarballs)
	bundle.uploadQueue = make(chan (TarBall), bundle.parallelTarballs+bundle.maxUploadQueue)
	for i := 0; i < bundle.parallelTarballs; i++ {
		bundle.NewTarBall(true)
		bundle.tarballQueue <- bundle.TarBall
	}
	bundle.started = true
}

func (bundle *Bundle) Deque() TarBall {
	if !bundle.started {
		panic("Trying to deque from not started Queue")
	}
	return <-bundle.tarballQueue
}

func (bundle *Bundle) FinishQueue() error {
	if !bundle.started {
		panic("Trying to stop not started Queue")
	}
	bundle.started = false

	// At this point no new tarballs should be put into uploadQueue
	for len(bundle.uploadQueue) > 0 {
		select {
		case otb := <-bundle.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	// We have to deque exactly this count of workers
	for i := 0; i < bundle.parallelTarballs; i++ {
		tb := <-bundle.tarballQueue
		if tb.TarWriter() == nil {
			// This had written nothing
			continue
		}
		err := tb.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalk: failed to close tarball")
		}
		tb.AwaitUploads()
	}
	return nil
}

func (bundle *Bundle) EnqueueBack(tarBall TarBall, parallelOpInProgress *bool) {
	if !*parallelOpInProgress {
		bundle.tarballQueue <- tarBall
	}
}

func (bundle *Bundle) CheckSizeAndEnqueueBack(tarBall TarBall) error {
	if tarBall.Size() > bundle.MinSize {
		bundle.mutex.Lock()
		defer bundle.mutex.Unlock()

		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "TarWalk: failed to close tarball")
		}

		bundle.uploadQueue <- tarBall
		for len(bundle.uploadQueue) > bundle.maxUploadQueue {
			select {
			case otb := <-bundle.uploadQueue:
				otb.AwaitUploads()
			default:
			}
		}

		bundle.NewTarBall(true)
		tarBall = bundle.TarBall
	}
	bundle.tarballQueue <- tarBall
	return nil
}

// NewTarBall starts writing new tarball
func (bundle *Bundle) NewTarBall(dedicatedUploader bool) {
	bundle.TarBall = bundle.TarBallMaker.Make(dedicatedUploader)
}

// GetIncrementBaseLsn returns LSN of previous backup
func (bundle *Bundle) GetIncrementBaseLsn() *uint64 { return bundle.IncrementFromLsn }

// GetIncrementBaseFiles returns list of Files from previous backup
func (bundle *Bundle) GetIncrementBaseFiles() BackupFileList { return bundle.IncrementFromFiles }

// CheckTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func (bundle *Bundle) CheckTimelineChanged(conn *pgx.Conn) bool {
	if bundle.Replica {
		timeline, err := readTimeline(conn)
		if err != nil {
			log.Printf("Unbale to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		// https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
		// Following check is the very pessimistic approach on replica backup invalidation
		if timeline != bundle.Timeline {
			log.Printf("Timeline has changed since backup start. Sentinel for the backup will not be uploaded.")
			return true
		}
	}
	return false
}

// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func (bundle *Bundle) StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, err error) {
	var name, lsnStr string
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", 0, queryRunner.Version, errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, bundle.Replica, err = queryRunner.StartBackup(backup)

	if err != nil {
		return "", 0, queryRunner.Version, err
	}
	lsn, err = ParseLsn(lsnStr)

	if bundle.Replica {
		name, bundle.Timeline, err = WALFileName(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, err
		}
	}
	return "base_" + name, lsn, queryRunner.Version, nil

}

// TarWalk walks files provided by the passed in directory
// and creates compressed tar members labeled as `part_00i.tar.lzo`.
//
// To see which files and directories are Skipped, please consult
// ExcludedFilenames. Excluded directories will be created but their
// contents will not be included in the tar bundle.
func (bundle *Bundle) TarWalk(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(path, " deleted dring filepath walk")
			return nil
		}
		return errors.Wrap(err, "TarWalk: walk failed")
	}

	if info.Name() == "pg_control" {
		bundle.Sentinel = &Sentinel{info, path}
	} else {
		err = HandleTar(bundle, path, info, &bundle.Crypter)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			return errors.Wrap(err, "TarWalk: handle tar failed")
		}
	}
	return nil
}

// HandleSentinel uploads the compressed tar file of `pg_control`. Will only be called
// after the rest of the backup is successfully uploaded to S3. Returns
// an error upon failure.
func (bundle *Bundle) HandleSentinel() error {
	fileName := bundle.Sentinel.Info.Name()
	info := bundle.Sentinel.Info
	path := bundle.Sentinel.path

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(&bundle.Crypter, "pg_control.tar." + Lz4FileExtension)
	tarWriter := tarBall.TarWriter()

	hdr, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to grab header info")
	}

	hdr.Name = strings.TrimPrefix(path, tarBall.Trim())
	fmt.Println(hdr.Name)

	err = tarWriter.WriteHeader(hdr)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to write header")
	}

	if info.Mode().IsRegular() {
		f, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "HandleSentinel: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: f,
			N: int64(hdr.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "HandleSentinel: copy failed")
		}

		tarBall.AddSize(hdr.Size)
		f.Close()
	}

	err = tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to close tarball")
	}

	return nil
}

// HandleLabelFiles creates the `backup_label` and `tablespace_map` Files and uploads
// it to S3 by stopping the backup. Returns error upon failure.
func (bundle *Bundle) HandleLabelFiles(conn *pgx.Conn) (uint64, error) {
	var lb string
	var sc string
	var lsnStr string

	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: Failed to build query runner.")
	}
	lb, sc, lsnStr, err = queryRunner.StopBackup()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to stop backup")
	}

	lsn, err := ParseLsn(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to parse finish LSN")
	}

	if queryRunner.Version < 90600 {
		return lsn, nil
	}

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(&bundle.Crypter)
	tarWriter := tarBall.TarWriter()

	lhdr := &tar.Header{
		Name:     "backup_label",
		Mode:     int64(0600),
		Size:     int64(len(lb)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(lhdr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(lb))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(lhdr.Name)

	shdr := &tar.Header{
		Name:     "tablespace_map",
		Mode:     int64(0600),
		Size:     int64(len(sc)),
		Typeflag: tar.TypeReg,
	}

	err = tarWriter.WriteHeader(shdr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to write header")
	}
	_, err = io.Copy(tarWriter, strings.NewReader(sc))
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: copy failed")
	}
	fmt.Println(shdr.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to close tarball")
	}

	return lsn, nil
}
