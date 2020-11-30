package internal

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/fs"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
)

const (
	TarPartitionFolderName = "/tar_partitions/"
	PgControlPath          = "/global/pg_control"
)

var UnwrapAll map[string]bool = nil

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

type NoBackupsFoundError struct {
	error
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// Backup contains information about a valid backup
// generated and uploaded by WAL-G.
type Backup struct {
	BaseBackupFolder storage.Folder
	Name             string
	SentinelDto      *BackupSentinelDto // used for storage query caching
}

func NewBackup(baseBackupFolder storage.Folder, name string) *Backup {
	return &Backup{baseBackupFolder, name, nil}
}

// StopSentinelPath returns sentinel path.
func (b *Backup) StopSentinelPath() string {
	return SentinelNameFromBackup(b.Name)
}

func (b *Backup) MetadataPath() string {
	return b.Name + "/" + utility.MetadataFileName
}

func (b *Backup) TarPartitionFolder() storage.Folder {
	return b.BaseBackupFolder.GetSubFolder(b.Name + TarPartitionFolderName)
}

// CheckExistence checks that the specified backup exists.
func (b *Backup) CheckExistence() (bool, error) {
	return b.BaseBackupFolder.Exists(b.StopSentinelPath())
}

func (b *Backup) TarNames() ([]string, error) {
	tarPartitionFolder := b.TarPartitionFolder()
	objects, _, err := tarPartitionFolder.ListFolder()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list b '%s' for deletion", b.Name)
	}
	result := make([]string, len(objects))
	for id, object := range objects {
		result[id] = object.GetName()
	}
	return result, nil
}

func (b *Backup) Sentinel() (BackupSentinelDto, error) {
	if b.SentinelDto != nil {
		return *b.SentinelDto, nil
	}
	sentinelDto := BackupSentinelDto{}
	sentinelDtoData, err := b.fetchSentinelData()
	if err != nil {
		return sentinelDto, err
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
	}
	b.SentinelDto = &sentinelDto
	return sentinelDto, nil
}

// TODO : unit tests
func (b *Backup) fetchSentinelData() ([]byte, error) {
	backupReaderMaker := NewStorageReaderMaker(b.BaseBackupFolder, b.StopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return make([]byte, 0), err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDtoData, errors.Wrap(err, "failed to fetch sentinel")
	}
	return sentinelDtoData, nil
}

func (b *Backup) FetchMeta() (ExtendedMetadataDto, error) {
	extendedMetadataDto := ExtendedMetadataDto{}
	backupReaderMaker := NewStorageReaderMaker(b.BaseBackupFolder, b.MetadataPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return extendedMetadataDto, err
	}
	extendedMetadataDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return extendedMetadataDto, errors.Wrap(err, "failed to fetch metadata")
	}

	err = json.Unmarshal(extendedMetadataDtoData, &extendedMetadataDto)
	return extendedMetadataDto, errors.Wrap(err, "failed to unmarshal metadata")
}

func checkDbDirectoryForUnwrap(dbDataDirectory string, sentinelDto BackupSentinelDto) error {
	if !sentinelDto.IsIncremental() {
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return newNonEmptyDbDataDirectoryError(dbDataDirectory)
		}
	} else {
		tracelog.DebugLogger.Println("DB data directory before increment:")
		_ = filepath.Walk(dbDataDirectory,
			func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					tracelog.DebugLogger.Println(path)
				}
				return nil
			})

		for fileName, fileDescription := range sentinelDto.Files {
			if fileDescription.IsSkipped {
				tracelog.DebugLogger.Printf("Skipped file %v\n", fileName)
			}
		}
	}

	if sentinelDto.TablespaceSpec != nil && !sentinelDto.TablespaceSpec.empty() {
		err := setTablespacePaths(*sentinelDto.TablespaceSpec)
		if err != nil {
			return err
		}
	}

	return nil
}

func setTablespacePaths(spec TablespaceSpec) error {
	basePrefix, ok := spec.BasePrefix()
	if !ok {
		return fmt.Errorf("Tablespace specification base path is not set\n")
	}
	err := fs.NewFolder(basePrefix, TablespaceFolder).EnsureExists()
	if err != nil {
		return fmt.Errorf("Error creating pg_tblspc folder %v\n", err)
	}
	for _, location := range spec.tablespaceLocations() {
		err := fs.NewFolder(location.Location, "").EnsureExists()
		if err != nil {
			return fmt.Errorf("Error creating folder for tablespace %v\n", err)
		}
		err = os.Symlink(location.Location, filepath.Join(basePrefix, location.Symlink))
		if err != nil {
			return fmt.Errorf("Error creating tablespace symkink %v\n", err)
		}
	}
	return nil
}

// check that directory is empty before unwrap
func (b *Backup) unwrapToEmptyDirectory(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	err := checkDbDirectoryForUnwrap(dbDataDirectory, sentinelDto)
	if err != nil {
		return err
	}

	return b.unwrapOld(dbDataDirectory, sentinelDto, filesToUnwrap, createIncrementalFiles)
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (b *Backup) unwrapOld(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesToUnwrap, createIncrementalFiles)
	tarsToExtract, pgControlKey, err := b.TarsToExtract(sentinelDto, filesToUnwrap, false)
	if err != nil {
		return err
	}

	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of b.
	needPgControl := IsPgControlRequired(b, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return newPgControlNotFoundError()
	}

	err = ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}

	if needPgControl {
		err = ExtractAll(tarInterpreter, []ReaderMaker{NewStorageReaderMaker(b.TarPartitionFolder(), pgControlKey)})
		if err != nil {
			return errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return nil
}

func IsPgControlRequired(backup *Backup, sentinelDto BackupSentinelDto) bool {
	re := regexp.MustCompile(`^([^_]+._{1}[^_]+._{1})`)
	walgBasebackupName := re.FindString(backup.Name) == ""
	needPgControl := walgBasebackupName || sentinelDto.IsIncremental()
	return needPgControl
}

func isDirectoryEmpty(directoryPath string) (bool, error) {
	var isEmpty = true

	searchLambda := func(path string, info os.FileInfo, err error) error {
		if path != directoryPath {
			isEmpty = false
			tracelog.InfoLogger.Printf("found file '%s' in directory: '%s'\n", path, directoryPath)
		}
		return nil
	}
	err := filepath.Walk(directoryPath, searchLambda)
	return isEmpty, errors.Wrapf(err, "can't check, that directory: '%s' is empty", directoryPath)
}

// TODO : init tests
func (b *Backup) TarsToExtract(sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool, skipRedundantTars bool) (tarsToExtract []ReaderMaker, pgControlKey string, err error) {
	tarNames, err := b.TarNames()
	if err != nil {
		return nil, "", err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]ReaderMaker, 0, len(tarNames))

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	for _, tarName := range tarNames {
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete b restoration.  But only if it
		// exists: it won't in the case of WAL-E b
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			if pgControlKey != "" {
				panic("expect only one pg_control tar name match")
			}
			pgControlKey = tarName
			continue
		}

		if skipRedundantTars && !shouldUnwrapTar(tarName, sentinelDto, filesToUnwrap) {
			continue
		}

		tarToExtract := NewStorageReaderMaker(b.TarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	return
}

func (b *Backup) GetFilesToUnwrap(fileMask string) (map[string]bool, error) {
	sentinelDto, err := b.Sentinel()
	if err != nil {
		return nil, err
	}
	if sentinelDto.Files == nil { // in case of WAL-E of old WAL-G b
		return UnwrapAll, nil
	}
	filesToUnwrap := make(map[string]bool)
	for file := range sentinelDto.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return utility.SelectMatchingFiles(fileMask, filesToUnwrap)
}

func shouldUnwrapTar(tarName string, sentinelDto BackupSentinelDto, filesToUnwrap map[string]bool) bool {
	if len(sentinelDto.TarFileSets) == 0 {
		return true
	}

	tarFiles := sentinelDto.TarFileSets[tarName]

	for _, file := range tarFiles {
		if filesToUnwrap[file] {
			return true
		}
	}

	tracelog.DebugLogger.Printf("Skipping archive '%s'\n", tarName)
	return false
}

func GetLastWalFilename(backup *Backup) (string, error) {
	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Print("No meta found.")
		return "", err
	}
	timelineId, err := ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		tracelog.InfoLogger.FatalError(err)
		return "", err
	}
	endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
	return endWalSegmentNo.getFilename(timelineId), nil
}

// returns set of filename related to this backup
func (b *Backup) BackupFilenames() map[string]struct{}{
	objects := make(map[string]struct{})

	/* delete backup */
	objects[b.Name] = struct{}{}
	/* delete sentinel for this backup */
	objects[SentinelNameFromBackup(b.Name)] = struct{}{}
	/* delete meta for backup */
	objects[b.MetadataPath()] = struct{}{}

	return objects
}

func (b *Backup) Delete(confirmed bool) error {
	objects := b.BackupFilenames()

	return storage.DeleteObjectsWhere(b.BaseBackupFolder, confirmed, func(object1 storage.Object) bool {
		_, ok := objects[object1.GetName()]
		return ok
	})
}
