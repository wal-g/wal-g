package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgconn"
	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

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
	Uploader              internal.Uploader
	isPermanent           bool
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
	userData              interface{}
	forceIncremental      bool
	backupsFolder         string
	pgDataDirectory       string
	isFullBackup          bool
	deltaConfigurator     DeltaBackupConfigurator
	withoutFilesMetadata  bool
	composerInitFunc      func(handler *BackupHandler) error
}

// CurBackupInfo holds all information that is harvest during the backup process
type CurBackupInfo struct {
	Name             string
	StartTime        time.Time
	startLSN         LSN
	endLSN           LSN
	uncompressedSize int64
	compressedSize   int64
	dataCatalogSize  int64
	incrementCount   int
}

func NewPrevBackupInfo(name string, sentinel BackupSentinelDto, filesMeta FilesMetadataDto) PrevBackupInfo {
	return PrevBackupInfo{
		name:             name,
		sentinelDto:      sentinel,
		filesMetadataDto: filesMeta,
	}
}

// PrevBackupInfo holds all information that is harvest during the backup process
type PrevBackupInfo struct {
	name             string
	sentinelDto      BackupSentinelDto
	filesMetadataDto FilesMetadataDto
}

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	Bundle      *Bundle
	QueryRunner *PgQueryRunner
}

// BackupPgInfo holds the PostgreSQL info that the handler queries before running the backup
type BackupPgInfo struct {
	pgVersion        int
	PgDataDirectory  string
	systemIdentifier *uint64
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	CurBackupInfo  CurBackupInfo
	prevBackupInfo PrevBackupInfo
	Arguments      BackupArguments
	Workers        BackupWorkers
	PgInfo         BackupPgInfo
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(uploader internal.Uploader, pgDataDirectory string, backupsFolder string, isPermanent bool,
	verifyPageChecksums bool, isFullBackup bool, storeAllCorruptBlocks bool, tarBallComposerType TarBallComposerType,
	deltaConfigurator DeltaBackupConfigurator, userData interface{}, withoutFilesMetadata bool) BackupArguments {
	return BackupArguments{
		Uploader:              uploader,
		pgDataDirectory:       pgDataDirectory,
		backupsFolder:         backupsFolder,
		isPermanent:           isPermanent,
		verifyPageChecksums:   verifyPageChecksums,
		isFullBackup:          isFullBackup,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
		deltaConfigurator:     deltaConfigurator,
		userData:              userData,
		withoutFilesMetadata:  withoutFilesMetadata,
		composerInitFunc: func(handler *BackupHandler) error {
			return configureTarBallComposer(handler, tarBallComposerType)
		},
	}
}

func (bh *BackupHandler) createAndPushBackup(ctx context.Context) {
	var err error
	folder := bh.Arguments.Uploader.Folder()
	// TODO: AB: this subfolder switch look ugly.
	// I think typed storage folders could be better (i.e. interface BasebackupStorageFolder, WalStorageFolder etc)
	bh.Arguments.Uploader.ChangeDirectory(bh.Arguments.backupsFolder)
	tracelog.DebugLogger.Printf("Uploading folder: %s", bh.Arguments.Uploader.Folder())

	arguments := bh.Arguments
	crypter := internal.ConfigureCrypter()
	bh.Workers.Bundle = NewBundle(bh.PgInfo.PgDataDirectory, crypter, bh.prevBackupInfo.name,
		bh.prevBackupInfo.sentinelDto.BackupStartLSN, bh.prevBackupInfo.filesMetadataDto.Files, arguments.forceIncremental,
		viper.GetInt64(internal.TarSizeThresholdSetting))

	err = bh.startBackup()
	tracelog.ErrorLogger.FatalOnError(err)
	bh.handleDeltaBackup(folder)
	tarFileSets := bh.uploadBackup()
	sentinelDto, filesMetaDto, err := bh.setupDTO(tarFileSets)
	tracelog.ErrorLogger.FatalOnError(err)
	bh.markBackups(folder, sentinelDto)
	bh.uploadMetadata(ctx, sentinelDto, filesMetaDto)

	// logging backup set Name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bh.CurBackupInfo.Name)
}

func (bh *BackupHandler) startBackup() (err error) {
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres.")
	conn, err := Connect()
	if err != nil {
		return
	}
	bh.Workers.QueryRunner, err = NewPgQueryRunner(conn)
	if err != nil {
		return fmt.Errorf("failed to build query runner: %v", err)
	}

	tracelog.DebugLogger.Println("Running StartBackup.")
	backupName, backupStartLSN, err := bh.Workers.Bundle.StartBackup(
		bh.Workers.QueryRunner, utility.CeilTimeUpToMicroseconds(time.Now()).String())
	if err != nil {
		return
	}
	bh.CurBackupInfo.startLSN = backupStartLSN
	bh.CurBackupInfo.Name = backupName
	tracelog.DebugLogger.Printf("Backup name: %s\nBackup start LSN: %s", backupName, backupStartLSN)
	bh.initBackupTerminator()
	return
}

func (bh *BackupHandler) handleDeltaBackup(folder storage.Folder) {
	if len(bh.prevBackupInfo.name) > 0 && bh.prevBackupInfo.sentinelDto.BackupStartLSN != nil {
		tracelog.InfoLogger.Println("Delta backup enabled")
		tracelog.DebugLogger.Printf("Previous backup: %s\nBackup start LSN: %d", bh.prevBackupInfo.name,
			bh.prevBackupInfo.sentinelDto.BackupStartLSN)
		if *bh.prevBackupInfo.sentinelDto.BackupFinishLSN > bh.CurBackupInfo.startLSN {
			tracelog.ErrorLogger.FatalOnError(newBackupFromFuture(bh.prevBackupInfo.name))
		}
		if bh.prevBackupInfo.sentinelDto.SystemIdentifier != nil &&
			bh.PgInfo.systemIdentifier != nil &&
			*bh.PgInfo.systemIdentifier != *bh.prevBackupInfo.sentinelDto.SystemIdentifier {
			tracelog.ErrorLogger.FatalOnError(newBackupFromOtherBD())
		}

		useWalDelta, _, err := configureWalDeltaUsage()
		tracelog.ErrorLogger.FatalOnError(err)

		if useWalDelta {
			err := bh.Workers.Bundle.DownloadDeltaMap(internal.NewFolderReader(folder.GetSubFolder(utility.WalPath)), bh.CurBackupInfo.startLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided " +
					"delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. "+
					"Fallback to full scan delta backup\n", err)
			}
		}
		bh.CurBackupInfo.Name = bh.CurBackupInfo.Name + "_D_" + utility.StripWalFileName(bh.prevBackupInfo.name)
		tracelog.DebugLogger.Printf("Suffixing Backup name with Delta info: %s", bh.CurBackupInfo.Name)
	}
}

func (bh *BackupHandler) setupDTO(tarFileSets internal.TarFileSets) (sentinelDto BackupSentinelDto,
	filesMeta FilesMetadataDto, err error) {
	var tablespaceSpec *TablespaceSpec
	if !bh.Workers.Bundle.TablespaceSpec.empty() {
		tablespaceSpec = &bh.Workers.Bundle.TablespaceSpec
	}
	sentinelDto = NewBackupSentinelDto(bh, tablespaceSpec)
	filesMeta.setFiles(bh.Workers.Bundle.GetFiles())
	filesMeta.TarFileSets = tarFileSets.Get()
	filesMeta.DatabasesByNames, err = bh.collectDatabaseNamesMetadata()
	return sentinelDto, filesMeta, err
}

func (bh *BackupHandler) markBackups(folder storage.Folder, sentinelDto BackupSentinelDto) {
	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order
	if bh.Arguments.isPermanent && sentinelDto.IsIncremental() {
		markBackupHandler := internal.NewBackupMarkHandler(NewGenericMetaInteractor(), folder)
		markBackupHandler.MarkBackup(bh.prevBackupInfo.name, true)
	}
}

func (bh *BackupHandler) SetComposerInitFunc(initFunc func(handler *BackupHandler) error) {
	bh.Arguments.composerInitFunc = initFunc
}

func configureTarBallComposer(bh *BackupHandler, tarBallComposerType TarBallComposerType) error {
	maker, err := NewTarBallComposerMaker(tarBallComposerType, bh.Workers.QueryRunner,
		bh.Arguments.Uploader, bh.CurBackupInfo.Name,
		NewTarBallFilePackerOptions(bh.Arguments.verifyPageChecksums, bh.Arguments.storeAllCorruptBlocks),
		bh.Arguments.withoutFilesMetadata)
	if err != nil {
		return err
	}

	return bh.Workers.Bundle.SetupComposer(maker)
}

func (bh *BackupHandler) uploadBackup() internal.TarFileSets {
	bundle := bh.Workers.Bundle
	// Start a new tar bundle, walk the pgDataDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	err := bundle.StartQueue(internal.NewStorageTarBallMaker(bh.CurBackupInfo.Name, bh.Arguments.Uploader))
	tracelog.ErrorLogger.FatalOnError(err)

	err = bh.Arguments.composerInitFunc(bh)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(bh.PgInfo.PgDataDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := bundle.FinishTarComposer()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Uploading pg_control ...")
	err = bundle.UploadPgControl(bh.Arguments.Uploader.Compression().FileExtension())
	tracelog.ErrorLogger.FatalOnError(err)

	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	tracelog.DebugLogger.Println("Stop backup and upload backup_label and tablespace_map")
	labelFilesTarBallName, labelFilesList, finishLsn, err := bundle.uploadLabelFiles(bh.Workers.QueryRunner)
	tracelog.ErrorLogger.FatalOnError(err)
	bh.CurBackupInfo.endLSN = finishLsn
	bh.CurBackupInfo.uncompressedSize = atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
	bh.CurBackupInfo.compressedSize, err = bh.Arguments.Uploader.UploadedDataSize()
	bh.CurBackupInfo.dataCatalogSize = atomic.LoadInt64(bundle.DataCatalogSize)
	tracelog.ErrorLogger.FatalOnError(err)
	tarFileSets.AddFiles(labelFilesTarBallName, labelFilesList)
	timelineChanged := bundle.checkTimelineChanged(bh.Workers.QueryRunner)
	tracelog.DebugLogger.Printf("Labelfiles tarball name: %s", labelFilesTarBallName)
	tracelog.DebugLogger.Printf("Number of label files: %d", len(labelFilesList))
	tracelog.DebugLogger.Printf("Finish LSN: %s", bh.CurBackupInfo.endLSN)
	tracelog.DebugLogger.Printf("Uncompressed size: %d", bh.CurBackupInfo.uncompressedSize)
	tracelog.DebugLogger.Printf("Compressed size: %d", bh.CurBackupInfo.compressedSize)

	// Wait for all uploads to finish.
	tracelog.DebugLogger.Println("Waiting for all uploads to finish")
	bh.Arguments.Uploader.Finish()
	if bh.Arguments.Uploader.Failed() {
		tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bh.CurBackupInfo.Name)
	}
	if timelineChanged {
		tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
	}
	return tarFileSets
}

// HandleBackupPush handles the backup being read from Postgres or filesystem and being pushed to the repository
// TODO : unit tests
func (bh *BackupHandler) HandleBackupPush(ctx context.Context) {
	bh.CurBackupInfo.StartTime = utility.TimeNowCrossPlatformUTC()

	if bh.Arguments.pgDataDirectory == "" {
		bh.handleBackupPushRemote(ctx)
	} else {
		bh.handleBackupPushLocal(ctx)
	}
}

func (bh *BackupHandler) handleBackupPushRemote(ctx context.Context) {
	if bh.Arguments.forceIncremental {
		tracelog.ErrorLogger.Println("Delta backup not available for remote backup.")
		tracelog.ErrorLogger.Fatal("To run delta backup, supply [db_directory].")
	}
	// If no arg is parsed, try to run remote backup using pglogrepl's BASE_BACKUP functionality
	tracelog.InfoLogger.Println("Running remote backup through Postgres connection.")
	tracelog.InfoLogger.Println("Features like delta backup and partial restore are disabled, there might be a performance impact.")
	tracelog.InfoLogger.Println("To run with local backup functionalities, supply [db_directory].")
	if bh.PgInfo.pgVersion < 110000 && !bh.Arguments.verifyPageChecksums {
		tracelog.InfoLogger.Println("VerifyPageChecksums=false is only supported for streaming backup since PG11")
		bh.Arguments.verifyPageChecksums = true
	}
	bh.createAndPushRemoteBackup(ctx)
}

func (bh *BackupHandler) handleBackupPushLocal(ctx context.Context) {
	{
		// The 'data' path provided on the command line must point at the same directory as the one listed by the Postgresql server.
		// If mismatched, this means we aren't connected to the correct server. This is a fatal error.
		fromCli := bh.Arguments.pgDataDirectory
		fromServer := bh.PgInfo.PgDataDirectory // that value is expected to already be absolute and "unsymlinked"
		if utility.AbsResolveSymlink(fromCli) != fromServer {
			tracelog.ErrorLogger.Fatalf("Data directory from command line '%s' is not the same as Postgres' one '%s'", fromCli, fromServer)
		}
	}

	folder := bh.Arguments.Uploader.Folder()
	baseBackupFolder := folder.GetSubFolder(bh.Arguments.backupsFolder)
	tracelog.DebugLogger.Printf("Base backup folder: %s", baseBackupFolder.GetPath())

	bh.checkPgVersionAndPgControl()

	if bh.Arguments.isFullBackup {
		tracelog.InfoLogger.Println("Doing full backup.")
	} else {
		var err error
		bh.prevBackupInfo, bh.CurBackupInfo.incrementCount, err = bh.Arguments.deltaConfigurator.Configure(
			folder, bh.Arguments.isPermanent)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	bh.createAndPushBackup(ctx)
}

func (bh *BackupHandler) createAndPushRemoteBackup(ctx context.Context) {
	var err error
	uploader := bh.Arguments.Uploader
	uploader.ChangeDirectory(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Uploading folder: %s", uploader.Folder())

	var tarFileSets internal.TarFileSets
	if bh.Arguments.withoutFilesMetadata {
		tarFileSets = internal.NewNopTarFileSets()
	} else {
		tarFileSets = internal.NewRegularTarFileSets()
	}

	baseBackup := bh.runRemoteBackup(ctx)
	tracelog.InfoLogger.Println("Updating metadata")
	bh.CurBackupInfo.startLSN = LSN(baseBackup.StartLSN)
	bh.CurBackupInfo.endLSN = LSN(baseBackup.EndLSN)

	bh.CurBackupInfo.uncompressedSize = baseBackup.UncompressedSize
	bh.CurBackupInfo.compressedSize, err = bh.Arguments.Uploader.UploadedDataSize()
	tracelog.ErrorLogger.FatalOnError(err)
	sentinelDto := NewBackupSentinelDto(bh, baseBackup.GetTablespaceSpec())
	filesMetadataDto := NewFilesMetadataDto(baseBackup.Files, tarFileSets)
	bh.CurBackupInfo.Name = baseBackup.BackupName()
	tracelog.InfoLogger.Println("Uploading metadata")
	bh.uploadMetadata(ctx, sentinelDto, filesMetadataDto)
	// logging backup set Name
	tracelog.InfoLogger.Printf("Wrote backup with name %s", bh.CurBackupInfo.Name)
}

func (bh *BackupHandler) uploadMetadata(ctx context.Context, sentinelDto BackupSentinelDto, filesMetaDto FilesMetadataDto) {
	curBackupName := bh.CurBackupInfo.Name
	meta := NewExtendedMetadataDto(bh.Arguments.isPermanent, bh.PgInfo.PgDataDirectory,
		bh.CurBackupInfo.StartTime, sentinelDto)

	err := bh.uploadExtendedMetadata(ctx, meta)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to upload metadata file for backup %s: %v", curBackupName, err)
	}
	err = bh.uploadFilesMetadata(ctx, filesMetaDto)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to upload files metadata for backup %s: %v", curBackupName, err)
	}
	err = internal.UploadSentinel(bh.Arguments.Uploader, NewBackupSentinelDtoV2(sentinelDto, meta), bh.CurBackupInfo.Name)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to upload sentinel file for backup %s: %v", curBackupName, err)
	}
}

func (bh *BackupHandler) collectDatabaseNamesMetadata() (DatabasesByNames, error) {
	databases := make(DatabasesByNames)
	err := bh.Workers.QueryRunner.ForEachDatabase(
		func(currentRunner *PgQueryRunner, db PgDatabaseInfo) error {
			var err error
			info := NewDatabaseObjectsInfo(uint32(db.Oid))

			info.Tables, err = currentRunner.getTables()
			if err != nil {
				return err
			}

			databases[db.Name] = *info
			return nil
		})

	return databases, err
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	// RemoteBackup is triggered by not passing PGDATA to wal-g,
	// and version cannot be read easily using replication connection.
	// Retrieve both with this helper function which uses a temp connection to postgres.

	pgInfo, err := getPgServerInfo()
	if err != nil {
		return nil, err
	}

	bh = &BackupHandler{
		Arguments: arguments,
		PgInfo:    pgInfo,
	}

	return bh, nil
}

func (bh *BackupHandler) runRemoteBackup(ctx context.Context) *StreamingBaseBackup {
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

	baseBackup := NewStreamingBaseBackup(bh.PgInfo.PgDataDirectory, viper.GetInt64(internal.TarSizeThresholdSetting), conn)
	var bundleFiles internal.BundleFiles
	if bh.Arguments.withoutFilesMetadata {
		bundleFiles = &internal.NopBundleFiles{}
	} else {
		bundleFiles = &internal.RegularBundleFiles{}
	}
	tracelog.InfoLogger.Println("Starting remote backup")
	err = baseBackup.Start(bh.Arguments.verifyPageChecksums, diskLimit)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Streaming remote backup")
	err = baseBackup.Upload(ctx, bh.Arguments.Uploader, bundleFiles)
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

func getPgServerInfo() (pgInfo BackupPgInfo, err error) {
	// Creating a temporary connection to read slot info and wal_segment_size
	tracelog.DebugLogger.Println("Initializing tmp connection to read Postgres info")
	tmpConn, err := Connect()
	if err != nil {
		return pgInfo, err
	}

	queryRunner, err := NewPgQueryRunner(tmpConn)
	if err != nil {
		return pgInfo, err
	}

	pgInfo.PgDataDirectory, err = queryRunner.GetDataDir()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.PgDataDirectory = utility.ResolveSymlink(pgInfo.PgDataDirectory)
	tracelog.DebugLogger.Printf("Datadir: %s", pgInfo.PgDataDirectory)

	err = queryRunner.getVersion()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.pgVersion = queryRunner.Version
	tracelog.DebugLogger.Printf("Postgres version: %d", queryRunner.Version)

	err = queryRunner.getSystemIdentifier()
	if err != nil {
		return pgInfo, err
	}
	pgInfo.systemIdentifier = queryRunner.SystemIdentifier
	tracelog.DebugLogger.Printf("Postgres SystemIdentifier: %d", queryRunner.Version)

	err = tmpConn.Close()
	if err != nil {
		return pgInfo, err
	}

	return pgInfo, err
}

// TODO : unit tests
func (bh *BackupHandler) uploadExtendedMetadata(ctx context.Context, meta ExtendedMetadataDto) (err error) {
	metaFile := storage.JoinPath(bh.CurBackupInfo.Name, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return internal.NewSentinelMarshallingError(metaFile, err)
	}
	tracelog.DebugLogger.Printf("Uploading metadata file (%s):\n%s", metaFile, dtoBody)
	return bh.Arguments.Uploader.Upload(ctx, metaFile, bytes.NewReader(dtoBody))
}

func (bh *BackupHandler) uploadFilesMetadata(ctx context.Context, filesMetaDto FilesMetadataDto) (err error) {
	if bh.Arguments.withoutFilesMetadata {
		tracelog.InfoLogger.Printf("Files metadata tracking is disabled, will not upload the %s", FilesMetadataName)
		return nil
	}

	dtoBody, err := json.Marshal(filesMetaDto)
	if err != nil {
		return err
	}
	return bh.Arguments.Uploader.Upload(ctx, getFilesMetadataPath(bh.CurBackupInfo.Name), bytes.NewReader(dtoBody))
}

func (bh *BackupHandler) checkPgVersionAndPgControl() {
	_, err := os.ReadFile(filepath.Join(bh.PgInfo.PgDataDirectory, PgControlPath))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PgControl file not found: %v\n", err)
	_, err = os.ReadFile(filepath.Join(bh.PgInfo.PgDataDirectory, "PG_VERSION"))
	tracelog.ErrorLogger.FatalfOnError(
		"It looks like you are trying to backup not pg_data. PG_VERSION file not found: %v\n", err)
}

func (bh *BackupHandler) initBackupTerminator() {
	errCh := make(chan error, 1)

	addSignalListener(errCh)
	addPgIsAliveChecker(bh.Workers.QueryRunner, errCh)

	terminator := NewBackupTerminator(bh.Workers.QueryRunner, bh.PgInfo.pgVersion, bh.PgInfo.PgDataDirectory)

	go func() {
		err := <-errCh
		tracelog.ErrorLogger.Printf("Error: %v, gracefully stopping the running backup...", err)
		terminator.TerminateBackup()
		tracelog.ErrorLogger.Fatal("Finished backup termination, will now exit")
	}()
}

func addSignalListener(errCh chan error) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sigCh
		errCh <- fmt.Errorf("received interruption signal: %s", sig)
	}()
}

func addPgIsAliveChecker(queryRunner *PgQueryRunner, errCh chan error) {
	if !viper.IsSet(internal.PgAliveCheckInterval) {
		return
	}
	stateUpdateInterval, err := internal.GetDurationSetting(internal.PgAliveCheckInterval)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Initializing the PG alive checker (interval=%s)...", stateUpdateInterval)
	pgWatcher := NewPgWatcher(queryRunner, stateUpdateInterval)

	go func() {
		err := <-pgWatcher.Err
		errCh <- fmt.Errorf("PG alive check failed: %v", err)
	}()
}
