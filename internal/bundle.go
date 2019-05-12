package internal

import (
	"archive/tar"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"strconv"
)

// It is made so to load big database files of size 1GB one by one
const (
	DefaultTarSizeThreshold = int64((1 << 30) - 1)
	PgControl               = "pg_control"
	BackupLabelFilename     = "backup_label"
	TablespaceMapFilename   = "tablespace_map"
)

type TarSizeError struct {
	error
}

func NewTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

func (err TarSizeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// ExcludedFilenames is a list of excluded members from the bundled backup.
var ExcludedFilenames = make(map[string]utility.Empty)

func init() {
	filesToExclude := []string{
		"pg_log", "pg_xlog", "pg_wal",                                                                        // Directories
		"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf",        // Files
		"pg_dynshmem", "pg_notify", "pg_replslot", "pg_serial", "pg_stat_tmp", "pg_snapshots", "pg_subtrans", // Directories
	}

	for _, filename := range filesToExclude {
		ExcludedFilenames[filename] = utility.Empty{}
	}
}

// A Bundle represents the directory to
// be walked. Contains at least one TarBall
// if walk has started. Each TarBall except for the last one will be at least
// TarSizeThreshold bytes. The Sentinel is used to ensure complete
// uploaded backups; in this case, pg_control is used as
// the sentinel.
type Bundle struct {
	ArchiveDirectory   string
	TarSizeThreshold   int64
	Sentinel           *Sentinel
	TarBall            TarBall
	TarBallMaker       TarBallMaker
	Crypter            Crypter
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

func getTarSizeThreshold() int64 {
	tarSizeThresholdString, ok := LookupConfigValue("WALG_TAR_SIZE_THRESHOLD")

	if !ok {
		return DefaultTarSizeThreshold
	}

	tarSizeThreshold, err := strconv.ParseInt(tarSizeThresholdString, 10, 64)

	if err != nil {
		return DefaultTarSizeThreshold
	}

	return tarSizeThreshold
}

// TODO: use DiskDataFolder
func NewBundle(archiveDirectory string, crypter Crypter, incrementFromLsn *uint64, incrementFromFiles BackupFileList) *Bundle {
	return &Bundle{
		ArchiveDirectory:   archiveDirectory,
		TarSizeThreshold:   getTarSizeThreshold(),
		Crypter:            crypter,
		IncrementFromLsn:   incrementFromLsn,
		IncrementFromFiles: incrementFromFiles,
		Files:              &sync.Map{},
	}
}

func (bundle *Bundle) GetFileRelPath(fileAbsPath string) string {
	return utility.GetFileRelativePath(fileAbsPath, bundle.ArchiveDirectory)
}

func (bundle *Bundle) GetFiles() *sync.Map { return bundle.Files }

func (bundle *Bundle) StartQueue() error {
	if bundle.started {
		panic("Trying to start already started Queue")
	}
	var err error
	bundle.parallelTarballs, err = utility.GetMaxUploadDiskConcurrency()
	if err != nil {
		return err
	}
	bundle.maxUploadQueue, err = utility.GetMaxUploadQueue()
	if err != nil {
		return err
	}

	bundle.tarballQueue = make(chan TarBall, bundle.parallelTarballs)
	bundle.uploadQueue = make(chan TarBall, bundle.parallelTarballs+bundle.maxUploadQueue)
	for i := 0; i < bundle.parallelTarballs; i++ {
		bundle.NewTarBall(true)
		bundle.tarballQueue <- bundle.TarBall
	}
	bundle.started = true
	return nil
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

	// At this point no new tarballs should be put into uploadQueue
	for len(bundle.uploadQueue) > 0 {
		select {
		case otb := <-bundle.uploadQueue:
			otb.AwaitUploads()
		default:
		}
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

// TODO : unit tests
// checkTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func (bundle *Bundle) checkTimelineChanged(conn *pgx.Conn) bool {
	if bundle.Replica {
		timeline, err := readTimeline(conn)
		if err != nil {
			tracelog.ErrorLogger.Printf("Unable to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		// https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
		// Following check is the very pessimistic approach on replica backup invalidation
		if timeline != bundle.Timeline {
			tracelog.ErrorLogger.Printf("Timeline has changed since backup start. Sentinel for the backup will not be uploaded.")
			return true
		}
	}
	return false
}

// TODO : unit tests
// StartBackup starts a non-exclusive base backup immediately. When finishing the backup,
// `backup_label` and `tablespace_map` contents are not immediately written to
// a file but returned instead. Returns empty string and an error if backup
// fails.
func (bundle *Bundle) StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, dataDir string, err error) {
	var name, lsnStr string
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return "", 0, queryRunner.Version, "", errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, bundle.Replica, dataDir, err = queryRunner.StartBackup(backup)

	if err != nil {
		return "", 0, queryRunner.Version, "", err
	}
	lsn, err = pgx.ParseLSN(lsnStr)

	if bundle.Replica {
		name, bundle.Timeline, err = getWalFilename(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, "", err
		}
	} else {
		bundle.Timeline, err = readTimeline(conn)
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't get current timeline because of error: '%v'\n", err)
		}
	}
	return "base_" + name, lsn, queryRunner.Version, dataDir, nil

}

// TODO : unit tests
// HandleWalkedFSObject walks files provided by the passed in directory
// and creates compressed tar members labeled as `part_00i.tar.*`, where '*' is compressor file extension.
//
// To see which files and directories are Skipped, please consult
// ExcludedFilenames. Excluded directories will be created but their
// contents will not be included in the tar bundle.
func (bundle *Bundle) HandleWalkedFSObject(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	if info.Name() == PgControl {
		bundle.Sentinel = &Sentinel{info, path}
	} else {
		err = bundle.handleTar(path, info)
		if err != nil {
			if err == filepath.SkipDir {
				return err
			}
			return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
		}
	}
	return nil
}

// TODO : unit tests
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

	fileInfoHeader.Name = bundle.GetFileRelPath(path)
	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if !excluded && info.Mode().IsRegular() {
		baseFiles := bundle.GetIncrementBaseFiles()
		baseFile, wasInBase := baseFiles[fileInfoHeader.Name]
		// It is important to take MTime before ReadIncrementalFile()
		time := info.ModTime()

		// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
		// For details see
		// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

		if wasInBase && (time.Equal(baseFile.MTime)) {
			// File was not changed since previous backup
			tracelog.DebugLogger.Println("Skiped due to unchanged modification time")
			bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: time})
			return nil
		}

		tarBall := bundle.Deque()
		tarBall.SetUp(bundle.Crypter)
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
		tarBall := bundle.Deque()
		tarBall.SetUp(bundle.Crypter)
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

// TODO : unit tests
// UploadPgControl should only be called
// after the rest of the backup is successfully uploaded to S3.
func (bundle *Bundle) UploadPgControl(compressorFileExtension string) error {
	fileName := bundle.Sentinel.Info.Name()
	info := bundle.Sentinel.Info
	path := bundle.Sentinel.path

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(bundle.Crypter, "pg_control.tar."+compressorFileExtension)
	tarWriter := tarBall.TarWriter()

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "UploadPgControl: failed to grab header info")
	}

	fileInfoHeader.Name = bundle.GetFileRelPath(path)
	tracelog.InfoLogger.Println(fileInfoHeader.Name)

	err = tarWriter.WriteHeader(fileInfoHeader) // TODO : what happens in case of irregular pg_control?
	if err != nil {
		return errors.Wrap(err, "UploadPgControl: failed to write header")
	}

	if info.Mode().IsRegular() {
		file, err := os.Open(path)
		if err != nil {
			return errors.Wrapf(err, "UploadPgControl: failed to open file %s\n", path)
		}

		lim := &io.LimitedReader{
			R: file,
			N: int64(fileInfoHeader.Size),
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "UploadPgControl: copy failed")
		}

		tarBall.AddSize(fileInfoHeader.Size)
		file.Close()
	}

	err = tarBall.CloseTar()
	return errors.Wrap(err, "UploadPgControl: failed to close tarball")
}

// TODO : unit tests
// UploadLabelFiles creates the `backup_label` and `tablespace_map` files by stopping the backup
// and uploads them to S3.
func (bundle *Bundle) UploadLabelFiles(conn *pgx.Conn) (uint64, error) {
	queryRunner, err := NewPgQueryRunner(conn)
	if err != nil {
		return 0, errors.Wrap(err, "UploadLabelFiles: Failed to build query runner.")
	}
	label, offsetMap, lsnStr, err := queryRunner.StopBackup()
	if err != nil {
		return 0, errors.Wrap(err, "UploadLabelFiles: failed to stop backup")
	}

	lsn, err := pgx.ParseLSN(lsnStr)
	if err != nil {
		return 0, errors.Wrap(err, "UploadLabelFiles: failed to parse finish LSN")
	}

	if queryRunner.Version < 90600 {
		return lsn, nil
	}

	bundle.NewTarBall(false)
	tarBall := bundle.TarBall
	tarBall.SetUp(bundle.Crypter)

	labelHeader := &tar.Header{
		Name:     BackupLabelFilename,
		Mode:     int64(0600),
		Size:     int64(len(label)),
		Typeflag: tar.TypeReg,
	}

	_, err = PackFileTo(tarBall, labelHeader, strings.NewReader(label))
	if err != nil {
		return 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", labelHeader.Name)
	}
	tracelog.InfoLogger.Println(labelHeader.Name)

	offsetMapHeader := &tar.Header{
		Name:     TablespaceMapFilename,
		Mode:     int64(0600),
		Size:     int64(len(offsetMap)),
		Typeflag: tar.TypeReg,
	}

	_, err = PackFileTo(tarBall, offsetMapHeader, strings.NewReader(offsetMap))
	if err != nil {
		return 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", offsetMapHeader.Name)
	}
	tracelog.InfoLogger.Println(offsetMapHeader.Name)

	err = tarBall.CloseTar()
	if err != nil {
		return 0, errors.Wrap(err, "UploadLabelFiles: failed to close tarball")
	}

	return lsn, nil
}

func (bundle *Bundle) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if bundle.DeltaMap == nil {
		return nil, nil
	}
	return bundle.DeltaMap.GetDeltaBitmapFor(filePath)
}

func (bundle *Bundle) DownloadDeltaMap(folder storage.Folder, backupStartLSN uint64) error {
	deltaMap := NewPagedFileDeltaMap()
	logSegNo := logSegNoFromLsn(*bundle.IncrementFromLsn + 1)
	logSegNo -= logSegNo % WalFileInDelta
	lastLogSegNo := logSegNoFromLsn(backupStartLSN) - 1
	walParser := walparser.NewWalParser()
	for ; logSegNo+(WalFileInDelta-1) <= lastLogSegNo; logSegNo += WalFileInDelta {
		deltaFilename := toDeltaFilename(formatWALFileName(bundle.Timeline, logSegNo))
		reader, err := downloadAndDecompressWALFile(folder, deltaFilename)
		if err != nil {
			return errors.Wrapf(err, "Error during delta file '%s' downloading.", deltaFilename)
		}
		deltaFile, err := LoadDeltaFile(reader)
		if err != nil {
			return errors.Wrapf(err, "Error during reading delta file '%s'", deltaFilename)
		}
		walParser = deltaFile.WalParser
		reader.Close()
		for _, location := range deltaFile.Locations {
			deltaMap.AddToDelta(location)
		}
	}
	// We don't consider the case when there is no delta files from previous backup,
	// because in such a case postgres do a WAL-Switch and first WAL file appears to be whole.
	for ; logSegNo <= lastLogSegNo; logSegNo++ {
		walFilename := formatWALFileName(bundle.Timeline, logSegNo)
		reader, err := downloadAndDecompressWALFile(folder, walFilename)
		if err != nil {
			return errors.Wrapf(err, "Error during wal file '%s' downloading", walFilename)
		}
		locations, err := extractLocationsFromWalFile(walParser, reader)
		if err != nil {
			return errors.Wrapf(err, "Error during extracting locations from wal file: '%s'", walFilename)
		}
		reader.Close()
		for _, location := range locations {
			deltaMap.AddToDelta(location)
		}
	}
	bundle.DeltaMap = deltaMap
	return nil
}

// TODO : unit tests
func (bundle *Bundle) packFileIntoTar(path string, info os.FileInfo, fileInfoHeader *tar.Header, wasInBase bool, tarBall TarBall) error {
	incrementBaseLsn := bundle.GetIncrementBaseLsn()
	isIncremented := incrementBaseLsn != nil && wasInBase && isPagedFile(info, path)
	var fileReader io.ReadCloser
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: info.ModTime()})
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", path)
		}
		fileReader, fileInfoHeader.Size, err = ReadIncrementalFile(path, info.Size(), *incrementBaseLsn, bitmap)
		switch err.(type) {
		case nil:
			fileReader = &ReadCascadeCloser{&io.LimitedReader{
				R: io.MultiReader(fileReader, &ZeroReader{}),
				N: int64(fileInfoHeader.Size),
			}, fileReader}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", fileInfoHeader.Name)
			isIncremented = false
			fileReader, err = startReadingFile(fileInfoHeader, info, path, fileReader)
			if err != nil {
				return err
			}
		default:
			return errors.Wrapf(err, "packFileIntoTar: failed reading incremental file '%s'\n", path)
		}
	} else {
		var err error
		fileReader, err = startReadingFile(fileInfoHeader, info, path, fileReader)
		if err != nil {
			return err
		}
	}
	defer fileReader.Close()

	bundle.GetFiles().Store(fileInfoHeader.Name, BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: info.ModTime()})

	packedFileSize, err := PackFileTo(tarBall, fileInfoHeader, fileReader)
	if err != nil {
		return errors.Wrap(err, "packFileIntoTar: operation failed")
	}

	if packedFileSize != fileInfoHeader.Size {
		return NewTarSizeError(packedFileSize, fileInfoHeader.Size)
	}

	return nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string, fileReader io.ReadCloser) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "packFileIntoTar: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := NewDiskLimitReader(file)
	fileReader = &ReadCascadeCloser{&io.LimitedReader{
		R: io.MultiReader(diskLimitedFileReader, &ZeroReader{}),
		N: int64(fileInfoHeader.Size),
	}, file}
	return fileReader, nil
}
