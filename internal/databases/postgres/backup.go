package postgres

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	PgControlPath     = "/global/pg_control"
	FilesMetadataName = "files_metadata.json"
)

var UnwrapAll map[string]bool

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

var patternPgBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?(_%[2]s)?", PatternTimelineAndLogSegNo, PatternLSN)
var regexpPgBackupName = regexp.MustCompile(patternPgBackupName)

// Backup contains information about a valid Postgres backup
// generated and uploaded by WAL-G.
type Backup struct {
	internal.Backup
	SentinelDto      *BackupSentinelDto // used for storage query caching
	FilesMetadataDto *FilesMetadataDto

	// Greenplum backups only
	AoFilesMetadataDto *AOFilesMetadataDTO
}

func ToPgBackup(source internal.Backup) (output Backup) {
	return Backup{
		Backup: source,
	}
}

func NewBackup(baseBackupFolder storage.Folder, name string) Backup {
	return Backup{
		Backup: internal.NewBackup(baseBackupFolder, name),
	}
}

func (backup *Backup) getTarPartitionFolder() storage.Folder {
	return backup.Folder.GetSubFolder(backup.Name + internal.TarPartitionFolderName)
}

func (backup *Backup) GetTarNames() ([]string, error) {
	tarPartitionFolder := backup.getTarPartitionFolder()
	objects, _, err := tarPartitionFolder.ListFolder()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list backup '%s' for deletion", backup.Name)
	}
	result := make([]string, len(objects))
	for id, object := range objects {
		result[id] = object.GetName()
	}
	return result, nil
}

func (backup *Backup) GetSentinel() (BackupSentinelDto, error) {
	if backup.SentinelDto != nil {
		return *backup.SentinelDto, nil
	}

	// this utility struct is used for compatibility reasons, since
	// previous WAL-G versions used to store the FilesMetadataDto in the s json
	s := struct {
		BackupSentinelDto
		DeprecatedSentinelFields
	}{}

	err := backup.FetchSentinel(&s)
	if err != nil {
		return BackupSentinelDto{}, err
	}

	backup.SentinelDto = &s.BackupSentinelDto

	err = backup.readDeprecatedFields(s.DeprecatedSentinelFields)
	if err != nil {
		return BackupSentinelDto{}, err
	}

	return s.BackupSentinelDto, nil
}

// TODO : unit tests
func (backup *Backup) readDeprecatedFields(fields DeprecatedSentinelFields) error {
	if backup.SentinelDto == nil {
		return fmt.Errorf("can't read deprecated fields: backup sentinel is not fetched")
	}

	// old versions of WAL-G used to store the FilesMetadata in the BackupSentinelDto
	if fields.Files != nil {
		backup.FilesMetadataDto = &fields.FilesMetadataDto
	}

	// old versions of WAL-G used to have DeltaFromLSN field instead of the DeltaLSN
	if fields.DeltaFromLSN != nil {
		backup.SentinelDto.IncrementFromLSN = fields.DeltaFromLSN
	}

	return nil
}

func (backup *Backup) GetSentinelAndFilesMetadata() (BackupSentinelDto, FilesMetadataDto, error) {
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return BackupSentinelDto{}, FilesMetadataDto{}, err
	}

	// FilesMetadataDto might be already fetched
	if backup.FilesMetadataDto != nil {
		return sentinel, *backup.FilesMetadataDto, nil
	}

	var filesMetadata FilesMetadataDto

	// skip the files metadata fetch if backup was taken without it
	if sentinel.FilesMetadataDisabled {
		tracelog.InfoLogger.Printf("Files metadata tracking was disabled, skipping the download of %s", FilesMetadataName)
		backup.FilesMetadataDto = &filesMetadata
		return sentinel, filesMetadata, nil
	}

	err = internal.FetchDto(backup.Folder, &filesMetadata, getFilesMetadataPath(backup.Name))
	if err != nil {
		// double-check that this is not V2 backup
		sentinelV2, err2 := backup.getSentinelV2()
		// there should be no error since old sentinel can be read as V2
		if err2 != nil {
			return BackupSentinelDto{}, FilesMetadataDto{}, fmt.Errorf("failed to fetch backup sentinel for version-check: %v, "+
				"tried to fetch backup files metadata but received an error: %v", err2, err)
		}
		if sentinelV2.Version >= 2 {
			// if sentinel has a version >= 2 files_metadata.json is a must
			return BackupSentinelDto{}, FilesMetadataDto{}, fmt.Errorf("failed to fetch files metadata: %w", err)
		}

		// it is OK to have missing files metadata because old WAL-G versions and WAL-E did not track it
		tracelog.WarningLogger.Printf(
			"Could not fetch any files metadata. Do you restore old or WAL-E backup? err: %v", err)
		filesMetadata = FilesMetadataDto{}
	}

	backup.FilesMetadataDto = &filesMetadata
	return sentinel, filesMetadata, nil
}

func (backup *Backup) getSentinelV2() (BackupSentinelDtoV2, error) {
	var sentinel BackupSentinelDtoV2

	err := backup.FetchSentinel(&sentinel)
	if err != nil {
		return BackupSentinelDtoV2{}, err
	}

	return sentinel, nil
}

func (backup *Backup) FetchMeta() (ExtendedMetadataDto, error) {
	extendedMetadataDto := ExtendedMetadataDto{}
	err := backup.FetchMetadata(&extendedMetadataDto)
	if err != nil {
		return ExtendedMetadataDto{}, err
	}

	return extendedMetadataDto, nil
}

// getFilesMetadataPath returns files metadata storage path.
func getFilesMetadataPath(backupName string) string {
	return backupName + "/" + FilesMetadataName
}

func checkDBDirectoryForUnwrap(dbDataDirectory string, sentinelDto BackupSentinelDto, filesMeta FilesMetadataDto) error {
	if !sentinelDto.IsIncremental() {
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		if err != nil {
			return err
		}
		if !isEmpty {
			return NewNonEmptyDBDataDirectoryError(dbDataDirectory)
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

		for fileName, fileDescription := range filesMeta.Files {
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
		return fmt.Errorf("tablespace specification base path is not set")
	}
	err := fs.NewFolder(basePrefix, TablespaceFolder).EnsureExists()
	if err != nil {
		return fmt.Errorf("error creating pg_tblspc folder %v", err)
	}
	for _, location := range spec.tablespaceLocations() {
		err := fs.NewFolder(location.Location, "").EnsureExists()
		if err != nil {
			return fmt.Errorf("error creating folder for tablespace %v", err)
		}
		err = os.Symlink(location.Location, filepath.Join(basePrefix, location.Symlink))
		if err != nil {
			return fmt.Errorf("error creating tablespace symkink %v", err)
		}
	}
	return nil
}

// check that directory is empty before unwrap
func (backup *Backup) unwrapToEmptyDirectory(
	dbDataDirectory string, sentinelDto BackupSentinelDto,
	filesMeta FilesMetadataDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	err := checkDBDirectoryForUnwrap(dbDataDirectory, sentinelDto, filesMeta)
	if err != nil {
		return err
	}

	return backup.unwrapOld(dbDataDirectory, sentinelDto, filesMeta, filesToUnwrap, createIncrementalFiles)
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (backup *Backup) unwrapOld(
	dbDataDirectory string, sentinelDto BackupSentinelDto,
	filesMeta FilesMetadataDto, filesToUnwrap map[string]bool, createIncrementalFiles bool,
) error {
	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesMeta, filesToUnwrap, createIncrementalFiles)
	tarsToExtract, pgControlKey, err := backup.getTarsToExtract(filesMeta, filesToUnwrap, false)
	if err != nil {
		return err
	}

	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	needPgControl := IsPgControlRequired(*backup, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return newPgControlNotFoundError()
	}

	err = internal.ExtractAll(tarInterpreter, tarsToExtract)
	if err != nil {
		return err
	}

	if needPgControl {
		err = internal.ExtractAll(tarInterpreter, []internal.ReaderMaker{
			internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), pgControlKey)})
		if err != nil {
			return errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return nil
}

func IsPgControlRequired(backup Backup, sentinelDto BackupSentinelDto) bool {
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
func (backup *Backup) getTarsToExtract(filesMeta FilesMetadataDto, filesToUnwrap map[string]bool,
	skipRedundantTars bool) (tarsToExtract []internal.ReaderMaker, pgControlKey string, err error) {
	tarNames, err := backup.GetTarNames()
	if err != nil {
		return nil, "", err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]internal.ReaderMaker, 0, len(tarNames))

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	for _, tarName := range tarNames {
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			if pgControlKey != "" {
				panic("expect only one pg_control tar name match")
			}
			pgControlKey = tarName
			continue
		}

		if skipRedundantTars && !shouldUnwrapTar(tarName, filesMeta, filesToUnwrap) {
			continue
		}

		tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}

	aoMeta, err := backup.LoadAoFilesMetadata()
	if err != nil {
		tracelog.DebugLogger.Printf("AO files metadata was not found. Skipping the AO segments unpacking.")
	} else {
		tracelog.InfoLogger.Printf("AO files metadata found. Will perform the AO segments unpacking.")
		for extractPath, meta := range aoMeta.Files {
			if !filesToUnwrap[extractPath] {
				tracelog.InfoLogger.Printf("Don't need to unwrap the %s AO segment file, skipping it...", extractPath)
				continue
			}
			objPath := path.Join(AoStoragePath, meta.StoragePath)
			readerMaker := internal.NewRegularFileStorageReaderMarker(backup.Folder, objPath, extractPath, meta.FileMode)
			tarsToExtract = append(tarsToExtract, readerMaker)
		}
	}

	return tarsToExtract, pgControlKey, nil
}

func (backup *Backup) GetFilesToUnwrap(fileMask string) (map[string]bool, error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	if err != nil {
		return nil, err
	}
	// in case of WAL-E of old WAL-G backup -or-
	// base backup created with WALG_WITHOUT_FILES_METADATA
	if len(filesMeta.Files) == 0 {
		return UnwrapAll, nil
	}
	filesToUnwrap := make(map[string]bool)
	for file := range filesMeta.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return utility.SelectMatchingFiles(fileMask, filesToUnwrap)
}

func (backup *Backup) LoadAoFilesMetadata() (*AOFilesMetadataDTO, error) {
	if backup.AoFilesMetadataDto != nil {
		return backup.AoFilesMetadataDto, nil
	}

	var meta AOFilesMetadataDTO
	err := internal.FetchDto(backup.Folder, &meta, getAOFilesMetadataPath(backup.Name))
	if err != nil {
		return nil, err
	}

	backup.AoFilesMetadataDto = &meta
	return backup.AoFilesMetadataDto, nil
}

func shouldUnwrapTar(tarName string, filesMeta FilesMetadataDto, filesToUnwrap map[string]bool) bool {
	// in case of base backup created with WALG_WITHOUT_FILES_METADATA
	if len(filesMeta.TarFileSets) == 0 {
		return true
	}

	tarFiles := filesMeta.TarFileSets[tarName]

	for _, file := range tarFiles {
		if filesToUnwrap[file] {
			return true
		}
	}

	tracelog.DebugLogger.Printf("Skipping archive '%s'\n", tarName)
	return false
}

func GetLastWalFilename(backup Backup) (string, error) {
	meta, err := backup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Print("No meta found.")
		return "", err
	}
	timelineID, err := ParseTimelineFromBackupName(backup.Name)
	if err != nil {
		tracelog.InfoLogger.FatalError(err)
		return "", err
	}
	endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
	return endWalSegmentNo.getFilename(timelineID), nil
}

func DeduceBackupName(object storage.Object) string {
	return regexpPgBackupName.FindString(object.GetName())
}
