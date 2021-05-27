package greenplum

import (
	"fmt"
	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jackc/pgx"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
	"regexp"
	"strings"
	"time"
)

type BackupArguments struct {
	isPermanent           bool
	verifyPageChecksums   bool
	storeAllCorruptBlocks bool
	userData              string
	pgDataDirectory       string
	isFullBackup          bool
	useRatingComposer     bool
	deltaFromUserData     string
	deltaFromName         string
}

type BackupWorkers struct {
	Uploader *internal.Uploader
	Conn     *pgx.Conn
}

type CurBackupInfo struct {
	backupName    string
	pgBackupNames []string
}

type BackupHandler struct {
	arguments     BackupArguments
	workers       BackupWorkers
	globalCluster *cluster.Cluster
	curBackupInfo CurBackupInfo
}

func (bh *BackupHandler) buildCommand(contentID int) string {
	segment := bh.globalCluster.ByContent[contentID][0]
	command := fmt.Sprintf("export PGPORT=%d && wal-g backup-push %s --backup-name-prefix %s_seg%d",
		segment.Port, segment.DataDir, bh.curBackupInfo.backupName, contentID)
	if bh.arguments.isPermanent {
		command += " --permanent"
	}
	if bh.arguments.verifyPageChecksums {
		command += " --verify"
	}
	if bh.arguments.isFullBackup {
		command += " --full"
	}
	if bh.arguments.storeAllCorruptBlocks {
		command += "--store-all-corrupt"
	}
	if bh.arguments.useRatingComposer {
		command += " --rating-composer"
	}
	if bh.arguments.deltaFromUserData != "" {
		command += " --delta-from-user-data " + bh.arguments.deltaFromUserData
	}
	if bh.arguments.deltaFromName != "" {
		backup := internal.NewBackup(bh.workers.Uploader.UploadingFolder, bh.curBackupInfo.backupName)
		sentinelDto := BackupSentinelDto{}
		err := backup.FetchSentinel(&sentinelDto)
		tracelog.ErrorLogger.FatalOnError(err)
		pgBackupName := regexp.MustCompile(fmt.Sprintf("^%s_seg%d_", bh.arguments.deltaFromName, contentID))

		for _, name := range *sentinelDto.BackupNames {
			matchedBackupName := pgBackupName.FindString(name)
			if matchedBackupName != "" {
				command += " --delta-from-name " + matchedBackupName
				break
			}
		}
	}
	if bh.arguments.userData != "" {
		command += " --add-user-data " + bh.arguments.userData
	}
	return command
}

func (bh *BackupHandler) HandleBackupPush() {
	folder := bh.workers.Uploader.UploadingFolder
	bh.workers.Uploader.UploadingFolder = folder.GetSubFolder(utility.BaseBackupPath)
	bh.curBackupInfo.backupName = "backup" + time.Now().Format("20060102150405")

	gplog.InitializeLogging("wal-g", "")
	remoteOutput := bh.globalCluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return bh.buildCommand(contentID)
		})
	bh.globalCluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})
	err := bh.connect()
	tracelog.ErrorLogger.FatalOnError(err)
	err = bh.createRestorePoint(bh.curBackupInfo.backupName)
	tracelog.ErrorLogger.FatalOnError(err)
	err = bh.extractPgBackupNames(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	sentinelDto := NewBackupSentinelDto(bh.curBackupInfo)
	err = internal.UploadSentinel(bh.workers.Uploader, sentinelDto, bh.curBackupInfo.backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", bh.curBackupInfo.backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
}

func (bh *BackupHandler) extractPgBackupNames(folder storage.Folder) (err error) {
	backupNames := make([]string, 0)
	objects, _, err := folder.ListFolder()
	if err != nil {
		return err
	}
	patternBackupSentinelName := fmt.Sprintf("%s_seg-?[0-9]+_base_%[2]s(_D_%[2]s)?_backup_stop_sentinel.json",
		bh.curBackupInfo.backupName, postgres.PatternTimelineAndLogSegNo)
	regexpBackupSentinelName := regexp.MustCompile(patternBackupSentinelName)
	for _, obj := range objects {
		matched := regexpBackupSentinelName.FindString(obj.GetName())
		if matched != "" {
			backupNames = append(backupNames, postgres.FetchPgBackupName(obj))
		}
	}
	bh.curBackupInfo.pgBackupNames = backupNames
	return
}

func (bh *BackupHandler) connect() (err error) {
	tracelog.DebugLogger.Println("Connecting to Postgres.")
	bh.workers.Conn, err = postgres.Connect()
	if err != nil {
		return
	}
	return
}

func (bh *BackupHandler) createRestorePoint(restorePointName string) (err error) {
	queryRunner, err := postgres.NewPgQueryRunner(bh.workers.Conn)
	if err != nil {
		return
	}
	_, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
	return
}

func getGpCluster() (globalCluster *cluster.Cluster, err error) {
	tracelog.DebugLogger.Println("Initializing tmp connection to read Greenplum info")
	tmpConn, err := postgres.Connect()
	if err != nil {
		return globalCluster, err
	}

	queryRunner, err := postgres.NewPgQueryRunner(tmpConn)
	if err != nil {
		return globalCluster, err
	}

	versionStr, err := queryRunner.GetGreenplumVersion()
	if err != nil {
		return globalCluster, err
	}
	versionStart := strings.Index(versionStr, "(Greenplum Database ") + len("(Greenplum Database ")
	versionEnd := strings.Index(versionStr, ")")
	versionStr = versionStr[versionStart:versionEnd]
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(versionStr)[0]
	semVer, err := semver.Make(threeDigitVersion)
	if err != nil {
		return globalCluster, err
	}

	segConfigs, err := queryRunner.GetGreenplumSegmentsInfo(semVer)
	if err != nil {
		return globalCluster, err
	}
	globalCluster = cluster.NewCluster(segConfigs)

	return globalCluster, nil
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return bh, err
	}
	globalCluster, err := getGpCluster()
	if err != nil {
		return bh, err
	}

	bh = &BackupHandler{
		arguments: arguments,
		workers: BackupWorkers{
			Uploader: uploader,
		},
		globalCluster: globalCluster,
	}
	return bh, err
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(pgDataDirectory string, isPermanent bool, verifyPageChecksums bool, isFullBackup bool,
	storeAllCorruptBlocks bool, useRatingComposer bool, deltaFromUserData string, deltaFromName string,
	userData string) BackupArguments {
	return BackupArguments{
		isPermanent:           isPermanent,
		verifyPageChecksums:   verifyPageChecksums,
		storeAllCorruptBlocks: storeAllCorruptBlocks,
		userData:              userData,
		pgDataDirectory:       pgDataDirectory,
		isFullBackup:          isFullBackup,
		useRatingComposer:     useRatingComposer,
		deltaFromUserData:     deltaFromUserData,
		deltaFromName:         deltaFromName,
	}
}
