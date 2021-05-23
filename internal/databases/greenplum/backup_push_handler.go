package greenplum

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
	"path/filepath"
	"sync/atomic"
	"time"
)

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	Uploader *postgres.WalUploader
	Bundles  []*postgres.Bundle
	Conn     *pgx.Conn
}

// BackupArguments holds all arguments parsed from cmd to this handler class
type BackupArguments struct {
	isPermanent           bool
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
	tarBallComposerType   postgres.TarBallComposerType
	userData              string
	ForceIncremental      bool
	BackupsFolder         string
	PgDataDirectories     []string
	IsFullBackup          bool
	deltaBaseSelector     internal.BackupSelector
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(pgDataDirectories []string, backupsFolder string, isPermanent bool, verifyPageChecksums bool,
	isFullBackup bool, storeAllCorruptBlocks bool, tarBallComposerType postgres.TarBallComposerType,
	deltaBaseSelector internal.BackupSelector, userData string) BackupArguments {
	return BackupArguments{
		PgDataDirectories:     pgDataDirectories,
		BackupsFolder:         backupsFolder,
		isPermanent:           isPermanent,
		verifyPageChecksums:   verifyPageChecksums,
		IsFullBackup:          isFullBackup,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
		tarBallComposerType:   tarBallComposerType,
		deltaBaseSelector:     deltaBaseSelector,
		userData:              userData,
	}
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	CurBackupInfo  []postgres.CurBackupInfo
	PrevBackupInfo []postgres.PrevBackupInfo
	Arguments      BackupArguments
	workers        BackupWorkers
	PgInfos        []postgres.BackupPgInfo
}

func (bh *BackupHandler) startBackup() (err error) {
	// Connect to postgres and start/finish a nonexclusive backup.
	tracelog.DebugLogger.Println("Connecting to Postgres.")

	bh.workers.Conn, err = postgres.Connect()
	if err != nil {
		return
	}

	tracelog.DebugLogger.Println("Running StartBackup.")

	timeStr := utility.CeilTimeUpToMicroseconds(time.Now()).String()
	backupName, backupStartLSN, err := bh.workers.Bundles[0].StartBackup(
		bh.workers.Conn, timeStr)
	bh.CurBackupInfo[0].StartLSN = backupStartLSN
	bh.CurBackupInfo[0].Name = backupName
	queryRunner, err := postgres.NewPgQueryRunner(bh.workers.Conn)
	tracelog.ErrorLogger.FatalOnError(err)
	names, lsnStrs, inRecoveryValues, err := queryRunner.StartBackupForGreenplumSegments(timeStr)
	tracelog.ErrorLogger.FatalOnError(err)

	for i, bundle := range bh.workers.Bundles {
		if i != 0 {
			backupName, backupStartLSN, err := bundle.StartBackupForGreenplumSegments(bh.workers.Conn, inRecoveryValues[i-1], lsnStrs[i-1], names[i-1])
			if err != nil {
				return
			}
			bh.CurBackupInfo[i].StartLSN = backupStartLSN
			bh.CurBackupInfo[i].Name = backupName
		}
	}
	return
}

func (bh *BackupHandler) createAndPushBackup() {
	folder := bh.workers.Uploader.UploadingFolder
	bh.workers.Uploader.UploadingFolder = folder.GetSubFolder(bh.Arguments.BackupsFolder)
	tracelog.DebugLogger.Printf("Uploading folder: %s", bh.workers.Uploader.UploadingFolder)

	arguments := bh.Arguments
	crypter := internal.ConfigureCrypter()

	bh.workers.Bundles = make([]*postgres.Bundle, 0)
	for i, info := range bh.PrevBackupInfo {
		bh.workers.Bundles = append(bh.workers.Bundles, postgres.NewBundle(bh.PgInfos[i].PgDataDirectory,
			crypter, info.SentinelDto.BackupStartLSN, info.SentinelDto.Files, arguments.ForceIncremental,
			viper.GetInt64(internal.TarSizeThresholdSetting)))
	}

	err := bh.startBackup()
	tracelog.ErrorLogger.FatalOnError(err)

	tarFileSets := bh.uploadBackup()
	sentinelDtoSet := bh.setupDTO(tarFileSets)
	bh.markBackups(folder, sentinelDtoSet)
	bh.uploadMetadata(sentinelDtoSet)
	err = bh.createRecoveryPoint(bh.CurBackupInfo[0].Name)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (bh *BackupHandler) markBackups(folder storage.Folder, sentinelDtoSet []postgres.BackupSentinelDto) {
	// If pushing permanent delta backup, mark all previous backups permanent
	// Do this before uploading current meta to ensure that backups are marked in increasing order

	for i, sentinelDto := range sentinelDtoSet {
		if bh.Arguments.isPermanent && sentinelDto.IsIncremental() {
			markBackupHandler := internal.NewBackupMarkHandler(postgres.NewGenericMetaInteractor(), folder)
			markBackupHandler.MarkBackup(bh.PrevBackupInfo[i].Name, true)
		}
	}
}

func (bh *BackupHandler) uploadMetadata(sentinelDtoSet []postgres.BackupSentinelDto) {
	for i, sentinelDto := range sentinelDtoSet {
		curBackupName := bh.CurBackupInfo[i].Name
		err := bh.uploadExtendedMetadata(sentinelDto, bh.PgInfos[i], bh.PrevBackupInfo[i], bh.CurBackupInfo[i])
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to upload metadata file for backup: %s %v", curBackupName, err)
			tracelog.ErrorLogger.FatalError(err)
		}
		err = internal.UploadSentinel(bh.workers.Uploader, sentinelDto, bh.CurBackupInfo[i].Name)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", curBackupName)
			tracelog.ErrorLogger.FatalError(err)
		}
	}
}

func (bh *BackupHandler) uploadExtendedMetadata(sentinelDto postgres.BackupSentinelDto, pgInfo postgres.BackupPgInfo,
	prevBackupInfo postgres.PrevBackupInfo, curBackupInfo postgres.CurBackupInfo) (err error) {
	meta := postgres.NewExtendedMetadataDto(bh.Arguments.isPermanent, pgInfo.PgDataDirectory,
		curBackupInfo.StartTime, sentinelDto)

	metaFile := storage.JoinPath(curBackupInfo.Name, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return internal.NewSentinelMarshallingError(metaFile, err)
	}
	tracelog.DebugLogger.Printf("Uploading metadata file (%s):\n%s", metaFile, dtoBody)
	return bh.workers.Uploader.Upload(metaFile, bytes.NewReader(dtoBody))
}

func (bh *BackupHandler) setupDTO(tarFileSets []postgres.TarFileSets) (sentinelDtos []postgres.BackupSentinelDto) {
	for i, tarFileSet := range tarFileSets {
		var tablespaceSpec *postgres.TablespaceSpec
		if !bh.workers.Bundles[i].TablespaceSpec.Empty() {
			tablespaceSpec = &bh.workers.Bundles[i].TablespaceSpec
		}
		sentinelDto := postgres.NewSentinelDto(bh.CurBackupInfo[i], bh.PrevBackupInfo[i], bh.PgInfos[i], bh.Arguments.userData, tablespaceSpec, tarFileSet)
		sentinelDto.SetFiles(bh.workers.Bundles[i].GetFiles())
		sentinelDtos = append(sentinelDtos, sentinelDto)
	}
	return sentinelDtos
}

func (bh *BackupHandler) uploadBackup() []postgres.TarFileSets {
	var tarFileSetsArr []postgres.TarFileSets
	for i, bundle := range bh.workers.Bundles {
		// Start a new tar bundle, walk the pgDataDirectory and upload everything there.
		tracelog.InfoLogger.Println("Starting a new tar bundle")
		err := bundle.StartQueue(internal.NewStorageTarBallMaker(bh.CurBackupInfo[i].Name, bh.workers.Uploader.Uploader))
		tracelog.ErrorLogger.FatalOnError(err)

		tarBallComposerMaker, err := postgres.NewTarBallComposerMaker(bh.Arguments.tarBallComposerType, bh.workers.Conn,
			postgres.NewTarBallFilePackerOptions(bh.Arguments.verifyPageChecksums, bh.Arguments.storeAllCorruptBlocks))
		tracelog.ErrorLogger.FatalOnError(err)

		err = bundle.SetupComposer(tarBallComposerMaker)
		tracelog.ErrorLogger.FatalOnError(err)

		tracelog.InfoLogger.Println("Walking ...")
		err = filepath.Walk(bh.PgInfos[i].PgDataDirectory, bundle.HandleWalkedFSObject)
		tracelog.ErrorLogger.FatalOnError(err)

		tracelog.InfoLogger.Println("Packing ...")
		tarFileSets, err := bundle.PackTarballs()
		tarFileSetsArr = append(tarFileSetsArr, tarFileSets)
		tracelog.ErrorLogger.FatalOnError(err)

		tracelog.DebugLogger.Println("Finishing queue ...")
		err = bundle.FinishQueue()
		tracelog.ErrorLogger.FatalOnError(err)

		tracelog.DebugLogger.Println("Uploading pg_control ...")
		err = bundle.UploadPgControl(bh.workers.Uploader.Compressor.FileExtension())
		tracelog.ErrorLogger.FatalOnError(err)
	}
	var labelFilesTarBallNames []string
	var labelFilesLists [][]string
	var finishLsns []uint64

	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	tracelog.DebugLogger.Println("Stop backup and upload backup_label and tablespace_map")
	labelFilesTarBallName, labelFilesList, finishLsn, err := bh.workers.Bundles[0].UploadLabelFiles(bh.workers.Conn)
	tracelog.ErrorLogger.FatalOnError(err)

	labelFilesTarBallNames = append(labelFilesTarBallNames, labelFilesTarBallName)
	labelFilesLists = append(labelFilesLists, labelFilesList)
	finishLsns = append(finishLsns, finishLsn)

	queryRunner, err := postgres.NewPgQueryRunner(bh.workers.Conn)
	tracelog.ErrorLogger.FatalOnError(err)

	labels, offsetMaps, lsnStrs, err := queryRunner.StopBackupForGreenplumSegments()
	tracelog.ErrorLogger.FatalOnError(err)

	for i, lsnStr := range lsnStrs {
		lsn, err := pgx.ParseLSN(lsnStr)
		tracelog.ErrorLogger.FatalOnError(err)

		if queryRunner.IsTablespaceMapExists() {
			labelFilesTarBallName, labelFilesList, finishLsn, err = bh.workers.Bundles[i+1].UploadLabelFilesForGreenplumSegment(labels[i], lsn, offsetMaps[i])
			tracelog.ErrorLogger.FatalOnError(err)
		} else {
			labelFilesTarBallName, labelFilesList, finishLsn = "", nil, lsn
		}
		labelFilesTarBallNames = append(labelFilesTarBallNames, labelFilesTarBallName)
		labelFilesLists = append(labelFilesLists, labelFilesList)
		finishLsns = append(finishLsns, finishLsn)
	}

	for i, bundle := range bh.workers.Bundles {
		bh.CurBackupInfo[i].EndLSN = finishLsn
		bh.CurBackupInfo[i].UncompressedSize = atomic.LoadInt64(bundle.TarBallQueue.AllTarballsSize)
		bh.CurBackupInfo[i].CompressedSize, err = bh.workers.Uploader.UploadedDataSize()
		tracelog.ErrorLogger.FatalOnError(err)
		tarFileSetsArr[i][labelFilesTarBallName] = append(tarFileSetsArr[i][labelFilesTarBallName], labelFilesList...)
		timelineChanged := bundle.CheckTimelineChanged(bh.workers.Conn)
		tracelog.DebugLogger.Printf("Labelfiles tarball name: %s", labelFilesTarBallName)
		tracelog.DebugLogger.Printf("Number of label files: %d", len(labelFilesList))
		tracelog.DebugLogger.Printf("Finish LSN: %d", bh.CurBackupInfo[i].EndLSN)
		tracelog.DebugLogger.Printf("Uncompressed size: %d", bh.CurBackupInfo[i].UncompressedSize)
		tracelog.DebugLogger.Printf("Compressed size: %d", bh.CurBackupInfo[i].CompressedSize)

		// Wait for all uploads to finish.
		tracelog.DebugLogger.Println("Waiting for all uploads to finish")
		bh.workers.Uploader.Finish()
		if bh.workers.Uploader.Failed.Load().(bool) {
			tracelog.ErrorLogger.Fatalf("Uploading failed during '%s' backup.\n", bh.CurBackupInfo[i].Name)
		}
		if timelineChanged {
			tracelog.ErrorLogger.Fatalf("Cannot finish backup because of changed timeline.")
		}
	}
	return tarFileSetsArr
}

func (bh *BackupHandler) HandleBackupPush() {
	folder := bh.workers.Uploader.UploadingFolder
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	tracelog.DebugLogger.Printf("Base backup folder: %s", baseBackupFolder)

	timeStr := utility.TimeNowCrossPlatformUTC()

	for i, pgSegmentInfo := range bh.PgInfos {
		bh.CurBackupInfo[i].StartTime = timeStr
		if utility.ResolveSymlink(bh.Arguments.PgDataDirectories[i]) != pgSegmentInfo.PgDataDirectory {
			tracelog.ErrorLogger.Panicf("Segment data directory read from Postgres (%s) is different then as parsed (%s).",
				bh.Arguments.PgDataDirectories[i], pgSegmentInfo.PgDataDirectory)
		}
	}

	bh.checkPgVersionAndPgControl()
	bh.createAndPushBackup()
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	uploader, err := postgres.ConfigureWalUploader()
	if err != nil {
		return bh, err
	}
	pgInfo, err := postgres.GetPgServerInfo()
	if err != nil {
		return bh, err
	}

	pgInfos := make([]postgres.BackupPgInfo, 0)
	pgInfos = append(pgInfos, pgInfo)

	segmentPgInfos, err := GetPgSegmentsInfo()
	pgInfos = append(pgInfos, segmentPgInfos...)
	if err != nil {
		return bh, err
	}

	for i, pgSegmentInfo := range pgInfos {
		if arguments.PgDataDirectories[i] != "" && arguments.PgDataDirectories[i] != pgSegmentInfo.PgDataDirectory {
			warning := fmt.Sprintf("Data directory for postgres on segment '%s' is not equal to backup-push argument '%s'",
				arguments.PgDataDirectories[i], pgInfo.PgDataDirectory)
			tracelog.WarningLogger.Println(warning)
		}
	}

	bh = &BackupHandler{
		Arguments: arguments,
		workers: BackupWorkers{
			Uploader: uploader,
		},
		PgInfos: pgInfos,
	}

	return bh, err
}

func GetPgSegmentsInfo() (pgInfos []postgres.BackupPgInfo, err error) {
	// Creating a temporary connection to read slot info and wal_segment_size
	tracelog.DebugLogger.Println("Initializing tmp connection to read Postgres info")
	tmpConn, err := postgres.Connect()
	if err != nil {
		return pgInfos, err
	}

	queryRunner, err := postgres.NewPgQueryRunner(tmpConn)
	if err != nil {
		return pgInfos, err
	}
	pgInfos = make([]postgres.BackupPgInfo, 0)

	dataDirs, err := queryRunner.GetSegmentDataDir()
	if err != nil {
		return pgInfos, err
	}
	ids, err := queryRunner.GetSegmentSystemIdentifiers()
	if err != nil {
		return pgInfos, err
	}
	versions, err := queryRunner.GetSegmentVersions()
	if err != nil {
		return pgInfos, err
	}
	for i := range versions {
		pgInfos = append(pgInfos, postgres.BackupPgInfo{
			PgVersion: versions[i], PgDataDirectory: dataDirs[i], SystemIdentifier: ids[i],
		})
	}

	err = tmpConn.Close()
	if err != nil {
		return pgInfos, err
	}

	return pgInfos, err
}

func (bh *BackupHandler) checkPgVersionAndPgControl() {
	for _, pgInfo := range bh.PgInfos {
		_, err := ioutil.ReadFile(filepath.Join(pgInfo.PgDataDirectory, postgres.PgControlPath))
		tracelog.ErrorLogger.FatalfOnError(
			"It looks like you are trying to backup not pg_data. PgControl file not found: %v\n", err)
		_, err = ioutil.ReadFile(filepath.Join(pgInfo.PgDataDirectory, "PG_VERSION"))
		tracelog.ErrorLogger.FatalfOnError(
			"It looks like you are trying to backup not pg_data. PG_VERSION file not found: %v\n", err)
	}
}

func (bh *BackupHandler) createRecoveryPoint(restorePointName string) (err error) {
	queryRunner, err := postgres.NewPgQueryRunner(bh.workers.Conn)
	if err != nil {
		return
	}
	_, err = queryRunner.CreateRestorePoint(restorePointName)
	return
}
