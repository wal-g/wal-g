package internal

import (
	"archive/tar"
	"fmt"
	"github.com/wal-g/wal-g/internal/walparser"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/viper"

	"io/ioutil"

	"github.com/RoaringBitmap/roaring"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
)

const (
	PgControl             = "pg_control"
	BackupLabelFilename   = "backup_label"
	TablespaceMapFilename = "tablespace_map"
	TablespaceFolder      = "pg_tblspc"
)

type TarSizeError struct {
	error
}

type PgDatabaseInfo struct {
	name      string
	oid       walparser.Oid
	tblSpcOid walparser.Oid
}

type PgStatRow struct {
	nTupleInserted uint64
	nTupleUpdated  uint64
	nTupleDeleted  uint64
}

func newTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

func (err TarSizeError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// ExcludedFilenames is a list of excluded members from the bundled backup.
var ExcludedFilenames = make(map[string]utility.Empty)

func init() {
	filesToExclude := []string{
		"log", "pg_log", "pg_xlog", "pg_wal", // Directories
		"pgsql_tmp", "postgresql.auto.conf.tmp", "postmaster.pid", "postmaster.opts", "recovery.conf", // Files
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
	AllTarballsSize    *int64
	Sentinel           *Sentinel
	TarBall            TarBall
	TarBallMaker       TarBallMaker
	Crypter            crypto.Crypter
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *uint64
	IncrementFromFiles BackupFileList
	DeltaMap           PagedFileDeltaMap
	TablespaceSpec     TablespaceSpec
	TableStatistics    map[walparser.RelFileNode]PgStatRow
	TarBallComposer    *TarBallComposer

	tarballQueue     chan TarBall
	uploadQueue      chan TarBall
	parallelTarballs int
	maxUploadQueue   int
	mutex            sync.Mutex
	started          bool
	forceIncremental bool

	Files *sync.Map
}

// TODO: use DiskDataFolder
func newBundle(
	archiveDirectory string, crypter crypto.Crypter,
	incrementFromLsn *uint64, incrementFromFiles BackupFileList,
	forceIncremental bool,
) *Bundle {
	tarSizeThreshold := viper.GetInt64(TarSizeThresholdSetting)
	return &Bundle{
		ArchiveDirectory:   archiveDirectory,
		TarSizeThreshold:   tarSizeThreshold,
		AllTarballsSize:    new(int64),
		Crypter:            crypter,
		IncrementFromLsn:   incrementFromLsn,
		IncrementFromFiles: incrementFromFiles,
		Files:              &sync.Map{},
		TablespaceSpec:     NewTablespaceSpec(archiveDirectory),
		forceIncremental:   forceIncremental,
		TarBallComposer:    NewTarBallComposer(uint64(tarSizeThreshold),
			NewDefaultComposeRatingEvaluator(incrementFromFiles)),
	}
}

func (bundle *Bundle) getFileRelPath(fileAbsPath string) string {
	return utility.PathSeparator + utility.GetSubdirectoryRelativePath(fileAbsPath, bundle.ArchiveDirectory)
}

func (bundle *Bundle) getFiles() *sync.Map { return bundle.Files }

func (bundle *Bundle) StartQueue() error {
	if bundle.started {
		panic("Trying to start already started Queue")
	}
	var err error
	bundle.parallelTarballs, err = getMaxUploadDiskConcurrency()
	if err != nil {
		return err
	}
	bundle.maxUploadQueue, err = getMaxUploadQueue()
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
		err := bundle.CloseTarball(tarBall)
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

		err := bundle.CloseTarball(tarBall)
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
func (bundle *Bundle) getIncrementBaseLsn() *uint64 { return bundle.IncrementFromLsn }

// GetIncrementBaseFiles returns list of Files from previous backup
func (bundle *Bundle) getIncrementBaseFiles() BackupFileList { return bundle.IncrementFromFiles }

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
func (bundle *Bundle) StartBackup(conn *pgx.Conn, backup string) (backupName string, lsn uint64, version int, dataDir string, systemIdentifier *uint64, err error) {
	var name, lsnStr string
	queryRunner, err := newPgQueryRunner(conn)
	if err != nil {
		return "", 0, 0, "", nil, errors.Wrap(err, "StartBackup: Failed to build query runner.")
	}
	name, lsnStr, bundle.Replica, dataDir, err = queryRunner.startBackup(backup)

	if err != nil {
		return "", 0, queryRunner.Version, "", queryRunner.SystemIdentifier, err
	}
	lsn, err = pgx.ParseLSN(lsnStr)

	if bundle.Replica {
		name, bundle.Timeline, err = getWalFilename(lsn, conn)
		if err != nil {
			return "", 0, queryRunner.Version, "", queryRunner.SystemIdentifier, err
		}
	} else {
		bundle.Timeline, err = readTimeline(conn)
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't get current timeline because of error: '%v'\n", err)
		}
	}
	return "base_" + name, lsn, queryRunner.Version, dataDir, queryRunner.SystemIdentifier, nil

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

	path, err = bundle.TablespaceSpec.makeTablespaceSymlinkPath(path)
	if err != nil {
		return fmt.Errorf("Could not make symlink path for location %s. %v\n", path, err)
	}
	isSymlink, err := bundle.TablespaceSpec.isTablespaceSymlink(path)
	if err != nil {
		return fmt.Errorf("Could not check whether path %s is symlink or not. %v\n", path, err)
	}
	if isSymlink {
		return nil
	}

	// Resolve symlinks for tablespaces and save folder structure.
	if filepath.Base(path) == TablespaceFolder {
		tablespaceInfos, err := ioutil.ReadDir(path)
		if err != nil {
			return fmt.Errorf("Could not read directory structure in %s: %v\n", TablespaceFolder, err)
		}
		for _, tablespaceInfo := range tablespaceInfos {
			if (tablespaceInfo.Mode() & os.ModeSymlink) != 0 {
				symlinkName := tablespaceInfo.Name()
				actualPath, err := os.Readlink(filepath.Join(path, symlinkName))
				if err != nil {
					return fmt.Errorf("Could not read symlink for tablespace %v\n", err)
				}
				bundle.TablespaceSpec.addTablespace(symlinkName, actualPath)
				err = filepath.Walk(actualPath, bundle.HandleWalkedFSObject)
				if err != nil {
					return fmt.Errorf("Could not walk tablespace symlink tree error %v\n", err)
				}
			}
		}
	}

	if info.Name() == PgControl {
		bundle.Sentinel = &Sentinel{info, path}
	} else {
		err = bundle.addToBundle(path, info)
		if err != nil {
			if err == filepath.SkipDir {
				return err
			}
			return errors.Wrap(err, "HandleWalkedFSObject: handle tar failed")
		}
	}
	return nil
}

// CollectStatistics collects statistics for each relFileNode
func (bundle *Bundle) CollectStatistics(conn *pgx.Conn) error {
	databases, err := getDatabaseInfos(conn)
	if err != nil {
		return errors.Wrap(err, "CollectStatistics: Failed to get db names.")
	}

	result := make(map[walparser.RelFileNode]PgStatRow)
	for _, db := range databases {
		databaseOption := func (c *pgx.ConnConfig) error {
			c.Database = db.name
			return nil
		}
		dbConn, err := Connect(databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to collect statistics for database: %s\n'%v'\n", db.name, err)
			continue
		}

		queryRunner, err := newPgQueryRunner(dbConn)
		if err != nil {
			return errors.Wrap(err, "CollectStatistics: Failed to build query runner.")
		}
		pgStatRows, err := queryRunner.getStatistics(&db)
		if err != nil {
			return errors.Wrap(err, "CollectStatistics: Failed to collect statistics.")
		}
		for relFileNode, statRow:= range pgStatRows {
			result[relFileNode] = statRow
		}
	}
	bundle.TableStatistics = result
	return nil
}

func getDatabaseInfos(conn *pgx.Conn) ([]PgDatabaseInfo, error) {
	queryRunner, err := newPgQueryRunner(conn)
	if err != nil {
		return nil, errors.Wrap(err, "getDatabaseInfos: Failed to build query runner.")
	}
	return queryRunner.getDatabaseInfos()
}

func (bundle *Bundle) getFileUpdateCount(filePath string) uint64 {
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		// TODO: try parse _vm, _fsm etc
		// and assign the update count from corresponding tables
		return 0
	}
	fileStat, ok := bundle.TableStatistics[*relFileNode]
	if !ok {
		return 0
	}
	return fileStat.nTupleDeleted + fileStat.nTupleUpdated + fileStat.nTupleInserted
}

func (bundle *Bundle) PackTarballs() (map[string][]string, error) {
	headers, tarFilesCollections := bundle.TarBallComposer.Compose()
	err := bundle.writeHeaders(headers)
	if err != nil {
		return nil, err
	}
	tarFileSets := make(map[string][]string, 0)
	for _, tarFilesCollection := range tarFilesCollections {
		tarBall := bundle.Deque()
		tarBall.SetUp(bundle.Crypter)
		for _, composeFileInfo := range tarFilesCollection.files {
			tarFileSets[tarBall.Name()] = append(tarFileSets[tarBall.Name()], composeFileInfo.header.Name)
		}
		// tarFilesCollection closure
		tarFilesCollectionLocal := tarFilesCollection
		go func() {
			for _, fileInfo := range tarFilesCollectionLocal.files {
				err := bundle.packFileIntoTar(fileInfo, tarBall)
				if err != nil {
					panic(err)
				}
			}
			err := bundle.FinishTarBall(tarBall)
			if err != nil {
				panic(err)
			}
		}()
	}

	return tarFileSets, nil
}

func (bundle *Bundle) FinishTarBall(tarBall TarBall) error {
	bundle.mutex.Lock()
	defer bundle.mutex.Unlock()
	err := tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "FinishTarBall: failed to close tarball")
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
	bundle.tarballQueue <- bundle.TarBall
	return nil
}

func (bundle *Bundle) writeHeaders(headers []*tar.Header) error {
	headersTarBall := bundle.Deque()
	headersTarBall.SetUp(bundle.Crypter)
	for _, header := range headers {
		err := headersTarBall.TarWriter().WriteHeader(header)
		if err != nil {
			return errors.Wrap(err, "addToBundle: failed to write header")
		}
	}
	bundle.EnqueueBack(headersTarBall)
	return nil
}

func (bundle *Bundle) getExpectedFileSize(filePath string, fileInfo os.FileInfo, wasInBase bool) (uint64, error) {
	incrementBaseLsn := bundle.getIncrementBaseLsn()
	isIncremented := incrementBaseLsn != nil && (wasInBase || bundle.forceIncremental) && isPagedFile(fileInfo, filePath)
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(filePath)
		if _, ok := err.(NoBitmapFoundError); ok {
			// this file has changed after the start of backup and will be skipped during file packing
			// so let the size be zero so it won't affect the calculations
			return 0, nil
		}
		if err != nil {
			return 0, errors.Wrapf(err, "getExpectedFileSize: failed to find corresponding bitmap '%s'\n", filePath)
		}
		if bitmap == nil {
			// if there was no bundle bitmap set, do a full scan instead to calculate expected changed blocks count?
			// as for now, just return size equal to entire page file size
			return uint64(fileInfo.Size()), nil
		}
		incrementBlocksCount := bitmap.GetCardinality()
		// expected header size = length(IncrementFileHeader) + sizeOf(fileSize) + sizeOf(diffBlockCount) + sizeOf(blockNo)*incrementBlocksCount
		incrementHeaderSize := uint64(len(IncrementFileHeader)) + sizeofInt64 + sizeofInt32 + (incrementBlocksCount * sizeofInt32)
		incrementPageDataSize := incrementBlocksCount * uint64(DatabasePageSize)
		return incrementHeaderSize + incrementPageDataSize, nil
	}
	return uint64(fileInfo.Size()), nil
}

// TODO : unit tests
// addToBundle handles one given file.
// Does not follow symlinks (it seems like it does). If file is in ExcludedFilenames, will not be included
// in the final tarball. EXCLUDED directories are created
// but their contents are not written to local disk.
func (bundle *Bundle) addToBundle(path string, info os.FileInfo) error {
	fileName := info.Name()
	_, excluded := ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}

	fileInfoHeader, err := tar.FileInfoHeader(info, fileName)
	if err != nil {
		return errors.Wrap(err, "addToBundle: could not grab header info")
	}

	fileInfoHeader.Name = bundle.getFileRelPath(path)
	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if !excluded && info.Mode().IsRegular() {
		baseFiles := bundle.getIncrementBaseFiles()
		baseFile, wasInBase := baseFiles[fileInfoHeader.Name]
		updatesCount := bundle.getFileUpdateCount(path)
		// It is important to take MTime before ReadIncrementalFile()
		time := info.ModTime()

		// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
		// For details see
		// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

		if (wasInBase || bundle.forceIncremental) && (time.Equal(baseFile.MTime)) {
			// File was not changed since previous backup
			tracelog.DebugLogger.Println("Skipped due to unchanged modification time")
			bundle.getFiles().Store(fileInfoHeader.Name,
				BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: time, UpdatesCount: updatesCount})
			return nil
		}
		expectedFileSize, err := bundle.getExpectedFileSize(path, info, wasInBase)
		if err != nil {
			return err
		}
		bundle.TarBallComposer.AddFile(path, info, wasInBase, fileInfoHeader, updatesCount, expectedFileSize)
	} else {
		bundle.TarBallComposer.AddHeader(fileInfoHeader)
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

	fileInfoHeader.Name = bundle.getFileRelPath(path)
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
		utility.LoggedClose(file, "")
	}

	err = bundle.CloseTarball(tarBall)
	return errors.Wrap(err, "UploadPgControl: failed to close tarball")
}

// TODO : unit tests
// UploadLabelFiles creates the `backup_label` and `tablespace_map` files by stopping the backup
// and uploads them to S3.
func (bundle *Bundle) uploadLabelFiles(conn *pgx.Conn) (map[string][]string, uint64, error) {
	queryRunner, err := newPgQueryRunner(conn)
	if err != nil {
		return nil, 0, errors.Wrap(err, "UploadLabelFiles: Failed to build query runner.")
	}
	label, offsetMap, lsnStr, err := queryRunner.stopBackup()
	if err != nil {
		return nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to stop backup")
	}

	lsn, err := pgx.ParseLSN(lsnStr)
	if err != nil {
		return nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to parse finish LSN")
	}

	if queryRunner.Version < 90600 {
		return nil, lsn, nil
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
		return nil, 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", labelHeader.Name)
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
		return nil, 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", offsetMapHeader.Name)
	}
	tracelog.InfoLogger.Println(offsetMapHeader.Name)
	tarFileSet := make(map[string][]string, 0)
	tarFileSet[tarBall.Name()] = []string{TablespaceMapFilename, BackupLabelFilename}
	err = bundle.CloseTarball(tarBall)
	if err != nil {
		return nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to close tarball")
	}

	return tarFileSet, lsn, nil
}

func (bundle *Bundle) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if bundle.DeltaMap == nil {
		return nil, nil
	}
	return bundle.DeltaMap.GetDeltaBitmapFor(filePath)
}

func (bundle *Bundle) DownloadDeltaMap(folder storage.Folder, backupStartLSN uint64) error {
	deltaMap, err := getDeltaMap(folder, bundle.Timeline, *bundle.IncrementFromLsn, backupStartLSN)
	if err != nil {
		return err
	}
	bundle.DeltaMap = deltaMap
	return nil
}

// TODO : unit tests
func (bundle *Bundle) packFileIntoTar(cfi *ComposeFileInfo, tarBall TarBall) error {
	incrementBaseLsn := bundle.getIncrementBaseLsn()
	isIncremented := incrementBaseLsn != nil && (cfi.wasInBase || bundle.forceIncremental) && isPagedFile(cfi.fileInfo, cfi.path)
	var fileReader io.ReadCloser
	if isIncremented {
		bitmap, err := bundle.getDeltaBitmapFor(cfi.path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			bundle.skipFile(cfi)
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "packFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.path)
		}
		fileReader, cfi.header.Size, err = ReadIncrementalFile(cfi.path, cfi.fileInfo.Size(), *incrementBaseLsn, bitmap)
		if os.IsNotExist(err) { // File was deleted before opening
			// We should ignore file here as if it did not exist.
			return nil
		}
		switch err.(type) {
		case nil:
			fileReader = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReader, &ioextensions.ZeroReader{}),
					N: cfi.header.Size,
				},
				Closer: fileReader,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.header.Name)
			isIncremented = false
			fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
			if err != nil {
				return err
			}
		default:
			return errors.Wrapf(err, "packFileIntoTar: failed reading incremental file '%s'\n", cfi.path)
		}
	} else {
		var err error
		fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
		if err != nil {
			return err
		}
	}
	defer utility.LoggedClose(fileReader, "")
	bundle.getFiles().Store(cfi.header.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented,
			MTime: cfi.fileInfo.ModTime(), UpdatesCount: cfi.updatesCount})

	packedFileSize, err := PackFileTo(tarBall, cfi.header, fileReader)
	if err != nil {
		return errors.Wrap(err, "packFileIntoTar: operation failed")
	}

	if packedFileSize != cfi.header.Size {
		return newTarSizeError(packedFileSize, cfi.header.Size)
	}

	return nil
}

func (bundle *Bundle) skipFile(cfi *ComposeFileInfo) {
	bundle.getFiles().Store(cfi.header.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false,
			MTime: cfi.fileInfo.ModTime(), UpdatesCount: cfi.updatesCount})
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string, fileReader io.ReadCloser) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "packFileIntoTar: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := NewDiskLimitReader(file)
	fileReader = &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: int64(fileInfoHeader.Size),
		},
		Closer: file,
	}
	return fileReader, nil
}

func (bundle *Bundle) CloseTarball(tarBall TarBall) error {
	atomic.AddInt64(bundle.AllTarballsSize, tarBall.Size())
	return tarBall.CloseTar()
}
