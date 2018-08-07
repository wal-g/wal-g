package walg

import (
	"archive/tar"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// It is made so to load big database files of size 1GB one by one
const DefaultTarSizeThreshold = int64((1 << 30) - 1)

// ExcludedFilenames is a list of excluded members from the bundled backup.
var ExcludedFilenames = make(map[string]Empty)

func init() {
	filesToExclude := []string{
		"pg_log", "pg_xlog", "pg_wal", // Directories
		"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf", // Files
		"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	}

	for _, filename := range filesToExclude {
		ExcludedFilenames[filename] = Empty{}
	}
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall except for the last one will be at least
// TarSizeThreshold bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	TarSizeThreshold   int64
	Sentinel           *Sentinel
	TarBall            TarBall
	TarBallMaker       TarBallMaker
	Crypter            OpenPGPCrypter
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList
	DeltaMap           PagedFileDeltaMap

	tarballQueue     chan TarBall
	uploadQueue      chan TarBall
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          bool

	Files *sync.Map
}

func NewBundle(incrementFromLsn *uint64, incrementFromFiles BackupFileList) *Bundle {
	return &Bundle{
		TarSizeThreshold:   DefaultTarSizeThreshold,
		IncrementFromLsn:   incrementFromLsn,
		IncrementFromFiles: incrementFromFiles,
		Files:              &sync.Map{},
	}
}

func (bundle *Bundle) GetFiles() *sync.Map { return bundle.Files }

func (bundle *Bundle) StartQueue() {
	if bundle.started {
		panic("Trying to start already started Queue")
	}
	bundle.parallelTarballs = getMaxUploadDiskConcurrency()
	bundle.maxUploadQueue = getMaxUploadQueue()
	bundle.tarballQueue = make(chan TarBall, bundle.parallelTarballs)
	bundle.uploadQueue = make(chan TarBall, bundle.parallelTarballs+bundle.maxUploadQueue)
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
		tarBall := <-bundle.tarballQueue
		if tarBall.TarWriter() == nil {
			// This had written nothing
			continue
		}
		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}
		tarBall.AwaitUploads()
	}
	return nil
}

func (bundle *Bundle) EnqueueBack(tarBall TarBall) {
	bundle.tarballQueue <- tarBall
}

func (bundle *Bundle) CheckSizeAndEnqueueBack(tarBall TarBall) error {
	if tarBall.Size() > bundle.TarSizeThreshold {
		bundle.mutex.Lock()
		defer bundle.mutex.Unlock()

		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
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

// checkTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func (bundle *Bundle) checkTimelineChanged(conn *pgx.Conn) bool {
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
	lsn, err = pgx.ParseLSN(lsnStr)

	if bundle.Replica {
		name, bundle.Timeline, err = walFileName(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, err
		}
	}
	return "base_" + name, lsn, queryRunner.Version, nil

}

// HandleWalkedFSObject walks files provided by the passed in directory
// and creates compressed tar members labeled as `part_00i.tar.*`, where '*' is compressor file extension.
//
// To see which files and directories are Skipped, please consult
// ExcludedFilenames. Excluded directories will be created but their
// contents will not be included in the tar bundle.
func (bundle *Bundle) HandleWalkedFSObject(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	if info.Name() == "pg_control" {
		bundle.Sentinel = &Sentinel{info, path}
	} else {
		err = bundle.handleTar(path, info)
		if err == filepath.SkipDir {
			return err
		}
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
		}
	}
	return nil
}

// handleTar creates underlying tar writer and handles one given file.
// Does not follow symlinks. If file is in ExcludedFilenames, will not be included
// in the final tarball. EXCLUDED directories are created
// but their contents are not written to local disk.
func (bundle *Bundle) handleTar(path string, info os.FileInfo) error {
	fileName := info.Name()
	_, excluded := ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "handleTar: could not grab header info")
	}

	tarBall := bundle.Deque() // TODO : simplify logic of it's returning back to the queue
	tarBall.SetUp(&bundle.Crypter)
	fileInfoHeader.Name = tarBall.GetFileRelPath(path)
	fmt.Println(fileInfoHeader.Name)

	if !excluded && info.Mode().IsRegular() {
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
			bundle.EnqueueBack(tarBall)
			return nil
		}

		go func() {
			// TODO: Refactor this functional mess
			// And maybe do a better error handling
			err := bundle.packFileIntoTar(path, info, fileInfoHeader, wasInBase, tarBall)
			if err != nil {
				panic(err)
			}
			err = bundle.CheckSizeAndEnqueueBack(tarBall)
			if err != nil {
				panic(err)
			}
		}()
	} else {
		defer bundle.EnqueueBack(tarBall)
		err = tarBall.TarWriter().WriteHeader(fileInfoHeader)
		if err != nil {
			return errors.Wrap(err, "handleTar: failed to write header")
		}
		if excluded && isDir {
			return filepath.SkipDir
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
	tarBall.SetUp(&bundle.Crypter, "pg_control.tar."+tarBall.FileExtension())
	tarWriter := tarBall.TarWriter()

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to grab header info")
	}

	fileInfoHeader.Name = tarBall.GetFileRelPath(path)
	fmt.Println(fileInfoHeader.Name)

	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		return errors.Wrap(err, "HandleSentinel: failed to write header")
	}

	if info.Mode().IsRegular() {
		file, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "HandleSentinel: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: file,
			N: int64(fileInfoHeader.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "HandleSentinel: copy failed")
		}

		tarBall.AddSize(fileInfoHeader.Size)
		file.Close()
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
	var label string
	var offsetMap string
	var lsnStr string

	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: Failed to build query runner.")
	}
	label, offsetMap, lsnStr, err = queryRunner.StopBackup()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to stop backup")
	}

	lsn, err := pgx.ParseLSN(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to parse finish LSN")
	}

	if queryRunner.Version < 90600 {
		return lsn, nil
	}

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(&bundle.Crypter)

	labelHeader := &tar.Header{
		Name:     "backup_label",
		Mode:     int64(0600),
		Size:     int64(len(label)),
		Typeflag: tar.TypeReg,
	}

	_, err = PackFileTo(tarBall, labelHeader, strings.NewReader(label))
	if err != nil {
		return 0, errors.Wrapf(err, "HandleLabelFiles: failed to put %s to tar", labelHeader.Name)
	}
	fmt.Println(labelHeader.Name)

	offsetMapHeader := &tar.Header{
		Name:     "tablespace_map",
		Mode:     int64(0600),
		Size:     int64(len(offsetMap)),
		Typeflag: tar.TypeReg,
	}

	_, err = PackFileTo(tarBall, offsetMapHeader, strings.NewReader(offsetMap))
	if err != nil {
		return 0, errors.Wrapf(err, "HandleLabelFiles: failed to put %s to tar", offsetMapHeader.Name)
	}
	fmt.Println(offsetMapHeader.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return 0, errors.Wrap(err, "HandleLabelFiles: failed to close tarball")
	}

	return lsn, nil
}

func (bundle *Bundle) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if bundle.DeltaMap == nil {
		return nil, nil
	}
	return bundle.DeltaMap.GetDeltaBitmapFor(filePath)
}

// TODO : unit tests
func (bundle *Bundle) DownloadDeltaMap(folder *S3Folder, backupStartLSN uint64) error {
	deltaMap := NewPagedFileDeltaMap()
	logSegNo := logSegNoFromLsn(*bundle.IncrementFromLsn + 1)
	logSegNo -= logSegNo % WalFileInDelta
	lastLogSegNo := logSegNoFromLsn(backupStartLSN)
	for ; logSegNo + (WalFileInDelta - 1) <= lastLogSegNo; logSegNo += WalFileInDelta {
		deltaFilename := toDeltaFilename(formatWALFileName(bundle.Timeline, logSegNo))
		reader, err := downloadAndDecompressWALFile(folder, deltaFilename)
		if err != nil {
			return err
		}
		locations, err := ReadLocationsFrom(reader)
		reader.Close()
		if err != nil {
			return err
		}
		for _, location := range locations {
			deltaMap.AddToDelta(location)
		}
	}
	for ; logSegNo <= lastLogSegNo; logSegNo++ {
		walFilename := formatWALFileName(bundle.Timeline, logSegNo)
		reader, err := downloadAndDecompressWALFile(folder, walFilename)
		if err != nil {
			return err
		}
		locations, err := extractLocationsFromWalFile(reader)
		reader.Close()
		if err != nil {
			return err
		}
		for _, location := range locations {
			deltaMap.AddToDelta(location)
		}
	}
	bundle.DeltaMap = deltaMap
	return nil
}

// TODO : unit tests
func (bundle *Bundle) packFileIntoTar(path string, info os.FileInfo, fileInfoHeader *tar.Header, wasInBase bool, tarBall TarBall) error {
	var err error
	incrementBaseLsn := bundle.GetIncrementBaseLsn()
	isIncremented := incrementBaseLsn != nil && wasInBase && isPagedFile(info, path) && !strings.Contains(path, GlobalTablespace)
	var fileReader io.ReadCloser
	var fileSize int64
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(path)
		if err != nil {
			return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", path)
		}
		fileReader, fileSize, err = ReadDatabaseFile(path, info.Size(), *incrementBaseLsn, bitmap)
	} else {
		fileSize = info.Size()
		fileReader, err = os.Open(path)
	}
	if err != nil {
		return errors.Wrapf(err, "packFileIntoTar: failed to open file '%s'\n", path)
	}
	defer fileReader.Close()

	fileInfoHeader.Size = fileSize

	bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: info.ModTime()})

	lim := &io.LimitedReader{
		R: io.MultiReader(fileReader, &ZeroReader{}),
		N: int64(fileInfoHeader.Size),
	}

	fileSize, err = PackFileTo(tarBall, fileInfoHeader, lim)
	if err != nil {
		return errors.Wrap(err, "packFileIntoTar: operation failed")
	}

	if fileSize != fileInfoHeader.Size {
		return errors.Errorf("packFileIntoTar: packed wrong numbers of bytes %d instead of %d", fileSize, fileInfoHeader.Size)
	}

	return nil
}
