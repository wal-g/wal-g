package postgres

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
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
	internal.Bundle
	Timeline           uint32
	Replica            bool
	IncrementFromLsn   *LSN
	IncrementFromFiles internal.BackupFileList
	IncrementFromName  string
	DeltaMap           PagedFileDeltaMap
	TablespaceSpec     TablespaceSpec
	DataCatalogSize    *int64

	forceIncremental bool
}

// TODO: use DiskDataFolder
func NewBundle(
	directory string, crypter crypto.Crypter,
	incrementFromName string, incrementFromLsn *LSN, incrementFromFiles internal.BackupFileList,
	forceIncremental bool, tarSizeThreshold int64,
) *Bundle {
	return &Bundle{
		Bundle: internal.Bundle{
			Directory:         directory,
			Crypter:           crypter,
			TarSizeThreshold:  tarSizeThreshold,
			ExcludedFilenames: ExcludedFilenames,
		},
		IncrementFromLsn:   incrementFromLsn,
		IncrementFromFiles: incrementFromFiles,
		IncrementFromName:  incrementFromName,
		TablespaceSpec:     NewTablespaceSpec(directory),
		forceIncremental:   forceIncremental,
		DataCatalogSize:    new(int64),
	}
}

func (bundle *Bundle) SetupComposer(composerMaker TarBallComposerMaker) (err error) {
	tarBallComposer, err := composerMaker.Make(bundle)
	if err != nil {
		return err
	}
	bundle.TarBallComposer = tarBallComposer
	return nil
}

// NewTarBall starts writing new tarball
func (bundle *Bundle) NewTarBall(dedicatedUploader bool) internal.TarBall {
	return bundle.TarBallQueue.NewTarBall(dedicatedUploader)
}

// GetIncrementBaseLsn returns LSN of previous backup
func (bundle *Bundle) getIncrementBaseLsn() *LSN { return bundle.IncrementFromLsn }

// GetIncrementBaseFiles returns list of Files from previous backup
func (bundle *Bundle) getIncrementBaseFiles() internal.BackupFileList {
	return bundle.IncrementFromFiles
}

// TODO : unit tests
// checkTimelineChanged compares timelines of pg_backup_start() and pg_backup_stop()
func (bundle *Bundle) checkTimelineChanged(queryRunner *PgQueryRunner) bool {
	if bundle.Replica {
		timeline, err := queryRunner.readTimeline()
		if err != nil {
			tracelog.ErrorLogger.Printf("Unable to check timeline change. Sentinel for the backup will not be uploaded.")
			return true
		}

		// Per discussion in
		//nolint:lll    // https://www.postgresql.org/message-id/flat/BF2AD4A8-E7F5-486F-92C8-A6959040DEB6%40yandex-team.ru#BF2AD4A8-E7F5-486F-92C8-A6959040DEB6@yandex-team.ru
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
func (bundle *Bundle) StartBackup(queryRunner *PgQueryRunner,
	backup string) (backupName string, lsn LSN, err error) {
	var name, lsnStr string
	name, lsnStr, bundle.Replica, err = queryRunner.startBackup(backup)

	if err != nil {
		return "", 0, err
	}
	lsn, err = ParseLSN(lsnStr)
	if err != nil {
		return "", 0, err
	}

	if bundle.Replica {
		name, bundle.Timeline, err = getWalFilename(lsn, queryRunner)
		if err != nil {
			return "", 0, err
		}
	} else {
		bundle.Timeline, err = queryRunner.readTimeline()
		if err != nil {
			tracelog.WarningLogger.Printf("Couldn't get current timeline because of error: '%v'\n", err)
		}
	}
	return "base_" + name, lsn, nil
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

	atomic.AddInt64(bundle.DataCatalogSize, info.Size())

	path, err = bundle.TablespaceSpec.makeTablespaceSymlinkPath(path)
	if err != nil {
		return fmt.Errorf("could not make symlink path for location %s. %v", path, err)
	}
	isSymlink, err := bundle.TablespaceSpec.isTablespaceSymlink(path)
	if err != nil {
		return fmt.Errorf("could not check whether path %s is symlink or not. %v", path, err)
	}
	if isSymlink {
		return nil
	}

	// Resolve symlinks for tablespaces and save folder structure.
	if filepath.Base(path) == TablespaceFolder {
		tablespaceEntries, err := os.ReadDir(path)
		if err != nil {
			return fmt.Errorf("could not read directory structure in %s: %v", TablespaceFolder, err)
		}
		for _, tablespaceEntry := range tablespaceEntries {
			if (tablespaceEntry.Type() & os.ModeSymlink) != 0 {
				symlinkName := tablespaceEntry.Name()
				actualPath, err := os.Readlink(filepath.Join(path, symlinkName))
				if err != nil {
					return fmt.Errorf("could not read symlink for tablespace %v", err)
				}
				bundle.TablespaceSpec.addTablespace(symlinkName, actualPath)
				err = filepath.Walk(actualPath, bundle.HandleWalkedFSObject)
				if err != nil {
					return fmt.Errorf("could not walk tablespace symlink tree error %v", err)
				}
			}
		}
	}

	if info.Name() == PgControl {
		bundle.Sentinel = &internal.Sentinel{Info: info, Path: path}
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

	fileInfoHeader.Name = bundle.GetFileRelPath(path)
	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if !excluded && info.Mode().IsRegular() {
		baseFiles := bundle.getIncrementBaseFiles()
		baseFile, wasInBase := baseFiles[fileInfoHeader.Name]
		// It is important to take MTime before ReadIncrementalFile()
		time := info.ModTime()

		// We do not rely here on monotonic time, instead we backup file if MTime changed somehow
		// For details see
		//nolint:lll    // https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru

		if (wasInBase || bundle.forceIncremental) && (time.Equal(baseFile.MTime)) {
			// File was not changed since previous backup
			tracelog.DebugLogger.Println("Skipped due to unchanged modification time: " + path)
			bundle.TarBallComposer.SkipFile(fileInfoHeader, info)
			return nil
		}
		incrementBaseLsn := bundle.getIncrementBaseLsn()
		isIncremented := incrementBaseLsn != nil && (wasInBase || bundle.forceIncremental) && isPagedFile(info, path)
		bundle.TarBallComposer.AddFile(internal.NewComposeFileInfo(path, info, wasInBase, isIncremented, fileInfoHeader))
	} else {
		err := bundle.TarBallComposer.AddHeader(fileInfoHeader, info)
		if err != nil {
			return err
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
	path := bundle.Sentinel.Path

	tarBall := bundle.NewTarBall(false)
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
			N: fileInfoHeader.Size,
		}

		_, err = io.Copy(tarWriter, lim)
		if err != nil {
			return errors.Wrap(err, "UploadPgControl: copy failed")
		}

		tarBall.AddSize(fileInfoHeader.Size)
		utility.LoggedClose(file, "")
	}

	err = bundle.TarBallQueue.CloseTarball(tarBall)
	return errors.Wrap(err, "UploadPgControl: failed to close tarball")
}

// TODO : unit tests
// UploadLabelFiles creates the `backup_label` and `tablespace_map` files by stopping the backup
// and uploads them to S3.
func (bundle *Bundle) uploadLabelFiles(queryRunner *PgQueryRunner) (string, []string, LSN, error) {
	label, offsetMap, lsnStr, err := queryRunner.stopBackup()
	if err != nil {
		return "", nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to stop backup")
	}

	lsn, err := ParseLSN(lsnStr)
	if err != nil {
		return "", nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to parse finish LSN")
	}

	if !queryRunner.IsTablespaceMapExists() {
		return "", nil, lsn, nil
	}

	tarBall := bundle.NewTarBall(false)
	tarBall.SetUp(bundle.Crypter)

	labelHeader := &tar.Header{
		Name:     BackupLabelFilename,
		Mode:     int64(0600),
		Size:     int64(len(label)),
		Typeflag: tar.TypeReg,
	}

	_, err = internal.PackFileTo(tarBall, labelHeader, strings.NewReader(label))
	if err != nil {
		return "", nil, 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", labelHeader.Name)
	}
	tracelog.InfoLogger.Println(labelHeader.Name)

	offsetMapHeader := &tar.Header{
		Name:     TablespaceMapFilename,
		Mode:     int64(0600),
		Size:     int64(len(offsetMap)),
		Typeflag: tar.TypeReg,
	}

	_, err = internal.PackFileTo(tarBall, offsetMapHeader, strings.NewReader(offsetMap))
	if err != nil {
		return "", nil, 0, errors.Wrapf(err, "UploadLabelFiles: failed to put %s to tar", offsetMapHeader.Name)
	}
	tracelog.InfoLogger.Println(offsetMapHeader.Name)

	err = bundle.TarBallQueue.CloseTarball(tarBall)
	if err != nil {
		return "", nil, 0, errors.Wrap(err, "UploadLabelFiles: failed to close tarball")
	}

	return tarBall.Name(), []string{TablespaceMapFilename, BackupLabelFilename}, lsn, nil
}

func (bundle *Bundle) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if bundle.DeltaMap == nil {
		return nil, nil
	}
	return bundle.DeltaMap.GetDeltaBitmapFor(filePath)
}

func (bundle *Bundle) DownloadDeltaMap(reader internal.StorageFolderReader, backupStartLSN LSN) error {
	deltaMap, err := getDeltaMap(reader, bundle.Timeline, *bundle.IncrementFromLsn, backupStartLSN)
	if err != nil {
		return err
	}
	bundle.DeltaMap = deltaMap
	return nil
}

func (bundle *Bundle) FinishTarComposer() (internal.TarFileSets, error) {
	return bundle.TarBallComposer.FinishComposing()
}

func (bundle *Bundle) GetFiles() *sync.Map {
	return bundle.TarBallComposer.GetFiles().GetUnderlyingMap()
}
