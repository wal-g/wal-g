package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/wal-g/wal-g/internal"

	"github.com/jackc/pgconn"

	"github.com/jackc/pgx"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type sentinelMarshallingError struct {
	error
}

func newSentinelMarshallingError(sentinelName string, err error) sentinelMarshallingError {
	return sentinelMarshallingError{errors.Wrapf(err, "Failed to marshall sentinel file: '%s'", sentinelName)}
}

func (err sentinelMarshallingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type backupFromFuture struct {
	error
}

func newBackupFromFuture(backupName string) backupFromFuture {
	return backupFromFuture{errors.Errorf("Finish LSN of backup %v greater than current LSN", backupName)}
}

func (err backupFromFuture) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type backupFromOtherBD struct {
	error
}

func newBackupFromOtherBD() backupFromOtherBD {
	return backupFromOtherBD{errors.Errorf("Current database and database of base backup are not equal.")}
}

func (err backupFromOtherBD) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// BackupArguments holds all arguments parsed from cmd to this handler class
type BackupArguments struct {
	isPermanent           bool
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
	tarBallComposerType   TarBallComposerType
	userData              string
	forceIncremental      bool
	backupsFolder         string
	pgDataDirectory       string
	isFullBackup          bool
	deltaBaseSelector     internal.BackupSelector
}

// BackupInfo holds all information that is harvest during the backup process
type BackupInfo struct {
	name             string
	sentinelDto      BackupSentinelDto
	meta             ExtendedMetadataDto
	startLSN         uint64
	endLSN           uint64
	uncompressedSize int64
	compressedSize   int64
	incrementCount   int
}

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	uploader *WalUploader
	bundle   *Bundle
	conn     *pgx.Conn
}

// BackupPgInfo holds the PostgreSQL info that the handler queries before running the backup
type BackupPgInfo struct {
	pgVersion        int
	pgDataDirectory  string
	systemIdentifier *uint64
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	curBackupInfo  BackupInfo
	prevBackupInfo BackupInfo
	arguments      BackupArguments
	workers        BackupWorkers
	pgInfo         BackupPgInfo
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(pgDataDirectory string, backupsFolder string, isPermanent bool, verifyPageChecksums bool,
	isFullBackup bool, storeAllCorruptBlocks bool, tarBallComposerType TarBallComposerType,
	deltaBaseSelector internal.BackupSelector, userData string) BackupArguments {
	return BackupArguments{
		pgDataDirectory:       pgDataDirectory,
		backupsFolder:         backupsFolder,
		isPermanent:           isPermanent,
		verifyPageChecksums:   verifyPageChecksums,
		isFullBackup:          isFullBackup,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
		tarBallComposerType:   tarBallComposerType,
		deltaBaseSelector:     deltaBaseSelector,
		userData:              userData,
	}
}

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	maxDeltas = viper.GetInt(internal.DeltaMaxStepsSetting)
	if origin, hasOrigin := internal.GetSetting(internal.DeltaOriginSetting); hasOrigin {
		switch origin {
		case internal.LatestString:
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatalf("Unknown %s: %s\n", internal.DeltaOriginSetting, origin)
		}
	}
	return
}

func (bc *BackupHandler) createAndPushBackup() {
	folder := bc.workers.uploader.UploadingFolder
	// TODO: AB: this subfolder switch look ugly.
	// I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)
	bc.workers.uploader.UploadingFolder = folder.GetSubFolder(bc.arguments.backupsFolder)
	tracelog.DebugLogger.Printf("Uploading folder: %s", bc.workers.uploader.UploadingFolder)

	arguments := bc.arguments
	meta := bc.curBackupInfo.meta
	crypter := internal.ConfigureCrypter()
	bc.workers.bundle = NewBundle(bc.pgInfo.pgDataDirectory, crypter, bc.prevBackupInfo.sentinelDto.BackupStartLSN,
		bc.prevBackupInfo.sentinelDto.Files, arguments.forceIncremental,
		viper.GetInt64(internal.TarSizeThresholdSetting))

	meta.StartTime = utility.TimeNowCrossPlatformUTC()
	meta.Hostname, _ = os.Hostname()
	meta.IsPermanent = arguments.isPermanent

	err := bc.startBackup()
	tracelog.ErrorLogger.FatalOnError(err)
	if meta.DataDir != bc.pgInfo.pgDataDirectory {
		warning := fmt.Sprintf("Data directory '%s' is not equal to backup-push argument '%s'",
			meta.DataDir, bc.pgInfo.pgDataDirectory)
		tracelog.WarningLogger.Println(warning)
	}
	bc.handleDeltaBackup(folder)
	tarFileSets := bc.uploadBackup()
	bc.handleTableSpaces(tarFileSets, folder)
}

func (bc *BackupHandler) startBackup() (err error) {
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres.")
	bc.workers.conn, err = Connect()
	if err != nil {
		return
	}

	tracelog.DebugLogger.Println("Running StartBackup.")
	backupName, backupStartLSN, pgVersion, dataDir, systemIdentifier, err := bc.workers.bundle.StartBackup(
		bc.workers.conn, utility.CeilTimeUpToMicroseconds(time.Now()).String())
	if err != nil {
		return
	}
	bc.pgInfo.pgVersion = pgVersion
	bc.curBackupInfo.startLSN = backupStartLSN
	bc.curBackupInfo.name = backupName
	bc.pgInfo.systemIdentifier = systemIdentifier
	tracelog.DebugLogger.Printf("Backup name: %s\nBackup start LSN: %d\nPostgres version: %d\nData dir: %s"+
		"\n System identifier: %d", backupName, backupStartLSN, pgVersion, dataDir, systemIdentifier)

	bc.curBackupInfo.meta.DataDir = dataDir
	return
}

func (bc *BackupHandler) handleDeltaBackup(folder storage.Folder) {
	if len(bc.prevBackupInfo.name) > 0 && bc.prevBackupInfo.sentinelDto.BackupStartLSN != nil {
		tracelog.InfoLogger.Println("Delta backup enabled")
		tracelog.DebugLogger.Printf("Previous backup: %s\nBackup start LSN: %d", bc.prevBackupInfo.name,
			bc.prevBackupInfo.sentinelDto.BackupStartLSN)
		if *bc.prevBackupInfo.sentinelDto.BackupFinishLSN > bc.curBackupInfo.startLSN {
			tracelog.ErrorLogger.FatalOnError(newBackupFromFuture(bc.prevBackupInfo.name))
		}
		if bc.prevBackupInfo.sentinelDto.SystemIdentifier != nil &&
			bc.pgInfo.systemIdentifier != nil &&
			*bc.pgInfo.systemIdentifier != *bc.prevBackupInfo.sentinelDto.SystemIdentifier {
			tracelog.ErrorLogger.FatalOnError(newBackupFromOtherBD())
		}
		if bc.workers.uploader.getUseWalDelta() {
			err := bc.workers.bundle.DownloadDeltaMap(folder.GetSubFolder(utility.WalPath), bc.curBackupInfo.startLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided " +
					"delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. "+
					"Fallback to full scan delta backup\n", err)
			}
		}
		bc.curBackupInfo.name = bc.curBackupInfo.name + "_D_" + utility.StripWalFileName(bc.prevBackupInfo.name)
		tracelog.DebugLogger.Printf("Suffixing Backup name with Delta info: %s", bc.curBackupInfo.name)
	}
}

func (bc *BackupHandler) handleTableSpaces(tarFileSets TarFileSets, folder storage.Folder) {
	var tablespaceSpec *TablespaceSpec
	if !bc.workers.bundle.TablespaceSpec.empty() {
		tablespaceSpec = &bc.workers.bundle.TablespaceSpec
	}
	bc.curBackupInfo.sentinelDto = NewBackupSentinelDto(bc, tablespaceSpec, tarFileSets)

	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if bc.arguments.isPermanent && bc.curBackupInfo.sentinelDto.IsIncremental() {
		markBackupHandler := internal.NewBackupMarkHandler(NewGenericMetaInteractor(), folder)
		markBackupHandler.MarkBackup(bc.prevBackupInfo.name, true)
	}

	bc.uploadMetadata()

	// logging backup set name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bc.curBackupInfo.name)
}

func (bc *BackupHandler) uploadBackup() TarFileSets {
	bundle := bc.workers.bundle
	// Start a new tar bundle, walk the pgDataDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bc.curBackupInfo.name, bc.workers.uploader.Uploader))
	tracelog.ErrorLogger.FatalOnError(err)

	tarBallComposerMaker, err := NewTarBallComposerMaker(bc.arguments.tarBallComposerType, bc.workers.conn,
		NewTarBallFilePackerOptions(bc.arguments.verifyPageChecksums, bc.arguments.storeAllCorruptBlocks))
	tracelog.ErrorLogger.FatalOnError(err)

	err = bundle.SetupComposer(tarBallComposerMaker)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(bc.pgInfo.pgDataDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := bundle.PackTarballs()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Uploading pg_control ...")
	err = bundle.UploadPgControl(bc.workers.uploader.Compressor.FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)

	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	tracelog.DebugLogger.Println("Stop backup and upload backup_label and tablespace_map")
	labelFilesTarBallName, labelFilesList, finishLsn, err := bundle.uploadLabelFiles(bc.workers.conn)
	tracelog.ErrorLogger.FatalOnError(err)
	bc.curBackupInfo.endLSN = finishLsn
	bc.curBackupInfo.uncompressedSize = atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	bc.curBackupInfo.compressedSize, err = bc.workers.uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets[labelFilesTarBallName] = append(tarFileSets[labelFilesTarBallName], labelFilesList...)
	timelineChanged := bundle.checkTimelineChanged(bc.workers.conn)
	tracelog.DebugLogger.Printf("Labelfiles tarball name: %s", labelFilesTarBallName)
	tracelog.DebugLogger.Printf("Number of label files: %d", len(labelFilesList))
	tracelog.DebugLogger.Printf("Finish LSN: %d", bc.curBackupInfo.endLSN)
	tracelog.DebugLogger.Printf("Uncompressed size: %d", bc.curBackupInfo.uncompressedSize)
	tracelog.DebugLogger.Printf("Compressed size: %d", bc.curBackupInfo.compressedSize)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	bc.workers.uploader.Finish()
	if bc.workers.uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bc.curBackupInfo.name)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}
	return tarFileSets
}

// HandleBackupPush handles the backup being read from Postgres or filesystem and being pushed to the repository
// TODO : unit tests
func (bc *BackupHandler) HandleBackupPush() {
	folder := bc.workers.uploader.UploadingFolder
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Base backup folder: %s", baseBackupFolder)

	if bc.arguments.pgDataDirectory == "" {
		if bc.arguments.forceIncremental {
			tracelog.ErrorLogger.Println("Delta backup not available for remote backup.")
			tracelog.ErrorLogger.Fatal("To run delta backup, supply [db_directory].")
		}
		// If no arg is parsed, try to run remote backup using pglogrepl's BASE_BACKUP functionality
		tracelog.InfoLogger.Println("Running remote backup through Postgres connection.")
		tracelog.InfoLogger.Println("Features like delta backup are disabled, there might be a performance impact.")
		tracelog.InfoLogger.Println("To run with local backup functionalities, supply [db_directory].")
		if bc.pgInfo.pgVersion < 110000 && !bc.arguments.verifyPageChecksums {
			tracelog.InfoLogger.Println("VerifyPageChecksums=false is only supported for streaming backup since PG11")
			bc.arguments.verifyPageChecksums = true
		}
		bc.createAndPushRemoteBackup()
		return
	}

	if utility.ResolveSymlink(bc.arguments.pgDataDirectory) != bc.pgInfo.pgDataDirectory {
		tracelog.ErrorLogger.Panicf("Data directory read from Postgres (%s) is different then as parsed (%s).",
			bc.arguments.pgDataDirectory, bc.pgInfo.pgDataDirectory)
	}
	bc.checkPgVersionAndPgControl()

	if bc.arguments.isFullBackup {
		tracelog.InfoLogger.Println("Doing full backup.")
	} else {
		err := bc.configureDeltaBackup()
		tracelog.ErrorLogger.FatalOnError(err)
	}

	bc.createAndPushBackup()
}

func (bc *BackupHandler) createAndPushRemoteBackup() {
	uploader := *bc.workers.uploader
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Uploading folder: %s", uploader.UploadingFolder)

	baseBackup := bc.runRemoteBackup()
	tracelog.InfoLogger.Println("Updating metadata")
	bc.updateMetaData(uint64(baseBackup.StartLSN), baseBackup)
	tracelog.InfoLogger.Println("Uploading metadata")
	bc.uploadMetadata()
	// logging backup set name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bc.curBackupInfo.name)
}

func (bc *BackupHandler) uploadMetadata() {
	curBackupName := bc.curBackupInfo.name
	err := bc.uploadMetadataFile()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", curBackupName, err)
		tracelog.ErrorLogger.FatalError(err)
	}
	err = bc.uploadSentinel()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", curBackupName)
		tracelog.ErrorLogger.FatalError(err)
	}
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bc *BackupHandler, err error) {
	// RemoteBackup is triggered by not passing PGDATA to wal-g,
	// and version cannot be read easily using replication connection.
	// Retrieve both with this helper function which uses a temp connection to postgres.

	uploader, err := ConfigureWalUploader()
	if err != nil {
		return bc, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return bc, err
	}
	pgInfo, err := getPgServerInfo()
	if err != nil {
		return bc, err
	}

	bc = &BackupHandler{
		arguments: arguments,
		workers: BackupWorkers{
			uploader: uploader,
		},
		pgInfo: pgInfo,
	}

	bc.curBackupInfo.meta = ExtendedMetadataDto{
		StartTime:   utility.TimeNowCrossPlatformUTC(),
		Hostname:    hostname,
		IsPermanent: arguments.isPermanent,
		DataDir:     pgInfo.pgDataDirectory,
	}
	return bc, err
}

func (bc *BackupHandler) runRemoteBackup() *StreamingBaseBackup {
	var diskLimit int32
	if viper.IsSet(internal.DiskRateLimitSetting) {
		// Note that BASE_BACKUP (pg protocol) allows to limit in kb/sec
		// Also note that the basebackup class  only enables this when set > 32kb/s
		diskLimit = int32(viper.GetInt64(internal.DiskRateLimitSetting)) / 1024
		if diskLimit > 32 {
			tracelog.InfoLogger.Printf("DiskIO limited to %d kb/s", diskLimit)
		}
	}
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres (replication connection)")
	conn, err := pgconn.Connect(context.Background(), "replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)

	baseBackup := NewStreamingBaseBackup(bc.pgInfo.pgDataDirectory, viper.GetInt64(internal.TarSizeThresholdSetting), conn)
	tracelog.InfoLogger.Println("Starting remote backup")
	err = baseBackup.Start(bc.arguments.verifyPageChecksums, diskLimit)
	tracelog.ErrorLogger.FatalOnError(err)

	backupStartLSN := uint64(baseBackup.StartLSN)
	bc.curBackupInfo.meta.StartLsn = backupStartLSN

	tracelog.InfoLogger.Println("Streaming remote backup")
	err = baseBackup.Upload(bc.workers.uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Finishing backup")
	tracelog.InfoLogger.Println("If wal-g hangs during this step, please Postgres log file for details.")
	err = baseBackup.Finish()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Closing Postgres connection (replication connection)")
	err = conn.Close(context.Background())
	tracelog.ErrorLogger.FatalOnError(err)
	return baseBackup
}

func (bc *BackupHandler) updateMetaData(startLSN uint64, bb *StreamingBaseBackup) {
	backupFinishLSN := uint64(bb.EndLSN)
	compressedSize, err := bc.workers.uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	bc.curBackupInfo.sentinelDto = BackupSentinelDto{
		BackupStartLSN:   &startLSN,
		PgVersion:        bc.pgInfo.pgVersion,
		BackupFinishLSN:  &backupFinishLSN,
		TablespaceSpec:   bb.GetTablespaceSpec(),
		UserData:         internal.GetSentinelUserData(),
		SystemIdentifier: &startLSN,
		UncompressedSize: bb.UncompressedSize,
		CompressedSize:   compressedSize,
		Files:            bb.Files,
	}
	meta := &bc.curBackupInfo.meta
	meta.SystemIdentifier = &startLSN
	meta.FinishLsn = uint64(bb.EndLSN)
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()
	bc.curBackupInfo.name = bb.BackupName()
}

func getPgServerInfo() (pgInfo BackupPgInfo, err error) {
	// Creating a temporary connection to read slot info and wal_segment_size
	tracelog.DebugLogger.Println("Initializing tmp connection to read Postgres info")
	tmpConn, err := Connect()
	if err != nil {
		return pgInfo, err
	}

	queryRunner, err := newPgQueryRunner(tmpConn)
	if err != nil {
		return pgInfo, err
	}

	pgInfo.pgDataDirectory, err = queryRunner.GetDataDir()
	if err != nil {
		return pgInfo, err
	}
	tracelog.DebugLogger.Printf("Datadir: %s", pgInfo.pgDataDirectory)

	err = queryRunner.getVersion()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.pgVersion = queryRunner.Version
	tracelog.DebugLogger.Printf("Postgres version: %d", queryRunner.Version)

	err = tmpConn.Close()
	if err != nil {
		return pgInfo, err
	}

	return pgInfo, err
}

func (bc *BackupHandler) configureDeltaBackup() (err error) {
	maxDeltas, fromFull := getDeltaConfig()
	if maxDeltas == 0 {
		return nil
	}

	folder := bc.workers.uploader.UploadingFolder
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	previousBackupName, err := bc.arguments.deltaBaseSelector.Select(folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return nil
		}
		return err
	}

	previousBackup := NewBackup(baseBackupFolder, previousBackupName)
	prevBackupSentinelDto, err := previousBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)

	if prevBackupSentinelDto.IncrementCount != nil {
		bc.curBackupInfo.incrementCount = *prevBackupSentinelDto.IncrementCount + 1
	} else {
		bc.curBackupInfo.incrementCount = 1
	}

	if bc.curBackupInfo.incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return nil
	}

	if prevBackupSentinelDto.BackupStartLSN == nil {
		tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. " +
			"Fallback to full backup with LSN marker for future deltas.")
		return nil
	}

	previousBackupMeta, err := previousBackup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Failed to get previous backup metadata: %s. Doing full backup.\n", err.Error())
		return nil
	}

	if !bc.arguments.isPermanent && !fromFull && previousBackupMeta.IsPermanent {
		tracelog.InfoLogger.Println("Can't do a delta backup from permanent backup. Doing full backup.")
		return nil
	}

	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")

		if prevBackupSentinelDto.IncrementFullName != nil {
			previousBackupName = *prevBackupSentinelDto.IncrementFullName
		}

		previousBackup := NewBackup(baseBackupFolder, previousBackupName)
		prevBackupSentinelDto, err = previousBackup.GetSentinel()
		if err != nil {
			return err
		}
	}
	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x.\n",
		previousBackupName, *prevBackupSentinelDto.BackupStartLSN)
	bc.prevBackupInfo.name = previousBackupName
	bc.prevBackupInfo.sentinelDto = prevBackupSentinelDto
	return nil
}

// TODO : unit tests
func (bc *BackupHandler) uploadMetadataFile() error {
	meta := &bc.curBackupInfo.meta
	sentinelDto := bc.curBackupInfo.sentinelDto
	// BackupSentinelDto struct allows nil field for backward compatibility
	// We do not expect here nil dto since it is new dto to upload
	meta.DatetimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"
	meta.FinishTime = utility.TimeNowCrossPlatformUTC()
	meta.StartLsn = *sentinelDto.BackupStartLSN
	meta.FinishLsn = *sentinelDto.BackupFinishLSN
	meta.PgVersion = sentinelDto.PgVersion
	meta.SystemIdentifier = sentinelDto.SystemIdentifier
	meta.UserData = sentinelDto.UserData
	meta.UncompressedSize = sentinelDto.UncompressedSize
	meta.CompressedSize = sentinelDto.CompressedSize

	metaFile := storage.JoinPath(bc.curBackupInfo.name, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return newSentinelMarshallingError(metaFile, err)
	}
	tracelog.DebugLogger.Printf("Uploading metadata file (%s):\n%s", metaFile, dtoBody)
	return bc.workers.uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

func (bc *BackupHandler) uploadSentinel() error {
	return UploadSentinel(bc.workers.uploader, bc.curBackupInfo.sentinelDto, bc.curBackupInfo.name)
}

// UploadSentinel takes care of uploading the sentinel file to the bucket
// TODO : unit tests
func UploadSentinel(uploader internal.UploaderProvider, sentinelDto interface{}, backupName string) error {
	sentinelName := SentinelNameFromBackup(backupName)

	dtoBody, err := json.Marshal(sentinelDto)
	if err != nil {
		return newSentinelMarshallingError(sentinelName, err)
	}
	tracelog.InfoLogger.Printf("Uploading sentinel file (%s)", sentinelName)
	return uploader.Upload(sentinelName, bytes.NewReader(dtoBody))
}

// SentinelNameFromBackup returns a sentinal name as derived from a backup name
func SentinelNameFromBackup(backupName string) string {
	return backupName + utility.SentinelSuffix
}

func (bc *BackupHandler) checkPgVersionAndPgControl() {
	_, err := ioutil.ReadFile(filepath.Join(bc.pgInfo.pgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PgControl file not found: %v\n", err)
	_, err = ioutil.ReadFile(filepath.Join(bc.pgInfo.pgDataDirectory, "PG_VERSION"))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PG_VERSION file not found: %v\n", err)
}
