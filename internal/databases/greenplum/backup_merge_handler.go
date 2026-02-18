package greenplum

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	conf "github.com/wal-g/wal-g/internal/config"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/utility"
)

const SegBackupMergeCmdName = "seg-backup-merge"

type BackupMergeArguments struct {
	Uploader           internal.Uploader
	targetBackupName   string
	segmentFwdArgs     []SegmentFwdArg
	logsDir            string
	segPollInterval    time.Duration
	segPollRetries     int
	segmentMetadataMap map[int]SegmentMetadata
	targetSentinel     BackupSentinelDto
	DoCleanup          bool // whether to cleanup old incremental chain and garbage after merge
}

type BackupMergeHandler struct {
	arguments            BackupMergeArguments
	backupSentinelDto    *BackupSentinelDto
	globalCluster        *cluster.Cluster
	conn                 *pgx.Conn
	backupPidByContentID map[int]int
}

func NewBackupMergeArguments(uploader internal.Uploader, targetBackupName string, fwdArgs []SegmentFwdArg, logsDir string,
	segPollInterval time.Duration, segPollRetries int, doCleanup bool) BackupMergeArguments {
	// First, fetch the target backup metadata to understand what segments need to be merged
	tracelog.InfoLogger.Printf("Fetching target backup metadata: %s", targetBackupName)
	rootFolder := uploader.Folder()
	targetBackup, err := NewBackup(rootFolder, targetBackupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("DEBUG: Failed to create target backup object: %v", err)
	}
	var targetSentinel BackupSentinelDto
	err = targetBackup.FetchSentinel(&targetSentinel)
	if err != nil {
		tracelog.ErrorLogger.Printf("DEBUG: FetchSentinel failed with error: %v", err)
	}
	tracelog.InfoLogger.Printf("Target backup has %d segments to merge", len(targetSentinel.Segments))

	// Convert segments slice to map with contentID as key
	segmentMetadataMap := make(map[int]SegmentMetadata)
	for _, segment := range targetSentinel.Segments {
		segmentMetadataMap[segment.ContentID] = segment
	}

	return BackupMergeArguments{
		Uploader:           uploader,
		targetBackupName:   targetBackupName,
		segmentFwdArgs:     fwdArgs,
		logsDir:            logsDir,
		segPollInterval:    segPollInterval,
		segPollRetries:     segPollRetries,
		segmentMetadataMap: segmentMetadataMap,
		targetSentinel:     targetSentinel,
		DoCleanup:          doCleanup,
	}
}

func NewBackupMergeHandler(arguments *BackupMergeArguments) (bmh *BackupMergeHandler, err error) {
	bmh = &BackupMergeHandler{
		arguments: *arguments,
	}

	// Connect to database to get cluster info
	conn, err := postgres.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	bmh.conn = conn

	// Get cluster info from database connection
	globalCluster, _, _, err := getGpClusterInfo(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info: %v", err)
	}
	bmh.globalCluster = globalCluster

	rootFolder := bmh.arguments.Uploader.Folder()

	// Fetch the original target backup to get its sentinel
	targetBackup, err := NewBackup(rootFolder, bmh.arguments.targetBackupName)
	if err != nil {
		return nil, fmt.Errorf("failed to create target backup object: %v", err)
	}

	var targetSentinel *BackupSentinelDto
	if err = targetBackup.FetchSentinel(&targetSentinel); err != nil {
		return nil, fmt.Errorf("failed to fetch target backup sentinel: %v", err)
	}

	bmh.backupSentinelDto = targetSentinel

	return bmh, nil
}

func (bmh *BackupMergeHandler) HandleBackupMerge() error {
	var err error
	tracelog.InfoLogger.Printf("Starting backup merge for target backup: %s", bmh.arguments.targetBackupName)
	defer bmh.disconnect()
	// Initialize gplog system
	initGpLog(bmh.arguments.logsDir)

	//start segments merge
	remoteOutput := bmh.globalCluster.GenerateAndExecuteCommand("Starting backup merge on segments",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return bmh.buildSegmentMergeCommand(contentID)
		})

	bmh.globalCluster.CheckClusterError(remoteOutput, "Unable to start segment merges", func(contentID int) string {
		return fmt.Sprintf("Unable to start merge on segment %d", contentID)
	}, true)

	for i := range remoteOutput.Commands {
		command := &remoteOutput.Commands[i]
		if command.Stderr != "" {
			tracelog.ErrorLogger.Printf("stderr (segment %d):\n%s\n", command.Content, command.Stderr)
		}
	}

	bmh.backupPidByContentID, err = extractBackupPids(remoteOutput)
	if err != nil {
		return fmt.Errorf("failed to extract backup PIDs: %v", err)
	}
	// this is a non-critical error since backup PIDs are only useful if backup is aborted
	tracelog.ErrorLogger.PrintOnError(err)
	if remoteOutput.NumErrors > 0 {
		return fmt.Errorf("encountered %d errors during segment merge start", remoteOutput.NumErrors)
	}

	err = bmh.waitSegmentMerges()
	if err != nil {
		return fmt.Errorf("waitSegmentMerges failed: %v", err)
	}
	tracelog.InfoLogger.Printf("SegmentMerges completed successfully")

	// create merged backup metadata
	mergedBackupName := strings.Split(bmh.arguments.targetBackupName, "_D_")[0]
	if err = bmh.createMergedBackupMetadata(mergedBackupName); err != nil {
		return fmt.Errorf("failed to create merged backup metadata: %v", err)
	}
	tracelog.InfoLogger.Printf("Backup merge completed successfully. Merged backup: %s", mergedBackupName)

	// post-merge cleanup: delete old incremental chain and purge garbage
	if bmh.arguments.DoCleanup {
		if err = bmh.cleanupAfterMerge(); err != nil {
			return fmt.Errorf("post-merge cleanup failed: %v", err)
		}
	} else {
		tracelog.InfoLogger.Printf("Cleanup after merge is disabled by flag; skipping deletion and garbage cleanup")
	}
	return nil
}

func (bmh *BackupMergeHandler) buildSegmentMergeCommand(contentID int) string {
	segment := bmh.globalCluster.ByContent[contentID][0]

	cmd := []string{
		// nohup to avoid the SIGHUP on SSH session disconnect
		"nohup", "wal-g seg-cmd-run",
		SegBackupMergeCmdName,
		bmh.arguments.segmentMetadataMap[contentID].BackupName,
		fmt.Sprintf("--content-id=%d", segment.ContentID),
		// actual arguments to be passed to the backup-push command
		// pass the config file location
		fmt.Sprintf("--config=%s", conf.CfgFile),
		// forward stdout and stderr to the log file
		"&>>", formatSegmentLogPath(contentID),
		// run in the background and get the launched process PID
		"& echo $!",
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.InfoLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

func (bmh *BackupMergeHandler) waitSegmentMerges() error {
	ticker := time.NewTicker(bmh.arguments.segPollInterval)
	retryCount := bmh.arguments.segPollRetries
	for {
		<-ticker.C
		states, err := bmh.pollSegmentMergeStates()
		if err != nil {
			if retryCount == 0 {
				return fmt.Errorf("gave up polling the backup-push states (tried %d times): %v", bmh.arguments.segPollRetries, err)
			}
			retryCount--
			tracelog.WarningLogger.Printf("failed to poll segment backup-push states, will try again %d more times", retryCount)
			continue
		}
		// reset retries after the successful poll
		retryCount = bmh.arguments.segPollRetries

		tracelog.InfoLogger.Printf("%v", states)

		runningMergeBackups, err := bmh.checkMergeStates(states)
		if err != nil {
			return err
		}

		if runningMergeBackups == 0 {
			tracelog.InfoLogger.Printf("No running merge backups left.")
			return nil
		}
	}
}

func (bmh *BackupMergeHandler) pollSegmentMergeStates() (map[int]SegCmdState, error) {
	segmentStates := make(map[int]SegCmdState)
	remoteOutput := bmh.globalCluster.GenerateAndExecuteCommand("Polling the segment backup-merge statuses...",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			cmd := fmt.Sprintf("cat %s", FormatCmdStatePath(contentID, SegBackupMergeCmdName))
			tracelog.InfoLogger.Printf("Command to run on segment %d: %s", contentID, cmd)
			return cmd
		})

	bmh.globalCluster.CheckClusterError(remoteOutput, "Unable to poll segment backup-merge states", func(contentID int) string {
		return fmt.Sprintf("Unable to poll backup-merge state on segment %d", contentID)
	}, true)

	for i := range remoteOutput.Commands {
		command := &remoteOutput.Commands[i]
		logger := tracelog.DebugLogger
		if command.Stderr != "" {
			logger = tracelog.WarningLogger
		}
		logger.Printf("Poll segment backup-merge state STDERR (segment %d):\n%s\n", command.Content, command.Stderr)
		logger.Printf("Poll segment backup-merge state STDOUT (segment %d):\n%s\n", command.Content, command.Stdout)
	}

	if remoteOutput.NumErrors > 0 {
		return nil, fmt.Errorf("encountered one or more errors during the polling. See %s for a complete list of errors",
			gplog.GetLogFilePath())
	}

	for i := range remoteOutput.Commands {
		command := &remoteOutput.Commands[i]
		mergeState := SegCmdState{}
		err := json.Unmarshal([]byte(command.Stdout), &mergeState)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal state JSON file: %v", err)
		}
		segmentStates[command.Content] = mergeState
	}

	return segmentStates, nil
}

func (bmh *BackupMergeHandler) checkMergeStates(states map[int]SegCmdState) (int, error) {
	runningBackupsCount := 0

	tracelog.InfoLogger.Printf("backup-merge states:")
	for contentID, state := range states {
		segments, ok := bmh.globalCluster.ByContent[contentID]
		if !ok || len(segments) != 1 {
			return 0, fmt.Errorf("failed to lookup the segment details for content ID %d", contentID)
		}
		host := segments[0].Hostname
		tracelog.InfoLogger.Printf("host: %s, content ID: %d, status: %s, ts: %s",
			host, contentID, state.Status, state.TS)
	}

	for contentID, state := range states {
		switch state.Status {
		case RunningCmdStatus:
			// give up if the heartbeat ts is too old
			if state.TS.Add(15 * time.Minute).Before(time.Now()) {
				return 0, fmt.Errorf("giving up waiting for segment %d: last seen on %s", contentID, state.TS)
			}
			runningBackupsCount++

		case FailedCmdStatus, InterruptedCmdStatus:
			return 0, fmt.Errorf("unexpected backup-merge status: %s on segment %d at %s", state.Status, contentID, state.TS)
		}
	}

	return runningBackupsCount, nil
}

func (bmh *BackupMergeHandler) disconnect() {
	tracelog.InfoLogger.Println("Disconnecting from the Greenplum master.")
	err := bmh.conn.Close(context.TODO())
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to disconnect: %v", err)
	}
}

func (bmh *BackupMergeHandler) createMergedBackupMetadata(mergedBackupName string) error {
	tracelog.InfoLogger.Printf("Creating merged backup metadata for: %s", mergedBackupName)
	rootFolder := bmh.arguments.Uploader.Folder()

	// Fetch the original target backup to get its sentinel
	targetBackup, err := NewBackup(rootFolder, bmh.arguments.targetBackupName)
	if err != nil {
		return fmt.Errorf("failed to create target backup object: %v", err)
	}

	var targetSentinel *BackupSentinelDto
	if err = targetBackup.FetchSentinel(&targetSentinel); err != nil {
		return fmt.Errorf("failed to fetch target backup sentinel: %v", err)
	}

	var targetRestorePoint RestorePointMetadata
	if targetRestorePoint, err = FetchRestorePointMetadata(rootFolder, bmh.arguments.targetBackupName); err != nil {
		return fmt.Errorf("failed to fetch target restorepoint sentinel: %v", err)
	}

	// Print target sentinel metadata for debugging
	tracelog.InfoLogger.Printf("Target CBDB Sentinel Metadata: %+v", targetSentinel)
	// Print target sentinel metadata for debugging
	tracelog.InfoLogger.Printf("Target CBDB Restore point Sentinel Metadata: %+v", targetRestorePoint)

	// Update segment metadata with merged backup names
	updatedSegments := make([]SegmentMetadata, 0, len(targetSentinel.Segments))
	for _, segMeta := range targetSentinel.Segments {
		segMeta.BackupName = strings.Split(segMeta.BackupName, "_D_")[0]
		updatedSegments = append(updatedSegments, segMeta)
	}

	// Create merged backup sentinel based on target backup
	mergedSentinel := BackupSentinelDto{
		RestorePoint:     targetSentinel.RestorePoint,
		Segments:         updatedSegments,
		UserData:         targetSentinel.UserData,
		StartTime:        targetSentinel.StartTime,
		FinishTime:       time.Now(),
		DatetimeFormat:   targetSentinel.DatetimeFormat,
		Hostname:         targetSentinel.Hostname,
		GpVersion:        targetSentinel.GpVersion,
		GpFlavor:         targetSentinel.GpFlavor,
		IsPermanent:      targetSentinel.IsPermanent,
		SystemIdentifier: targetSentinel.SystemIdentifier,
		UncompressedSize: targetSentinel.UncompressedSize,
		CompressedSize:   targetSentinel.CompressedSize,
		DataCatalogSize:  targetSentinel.DataCatalogSize,
		// Clear increment fields since this is now a merged (base) backup
		IncrementFrom:     nil,
		IncrementFullName: nil,
		IncrementCount:    nil,
	}

	// upload merged sentinel without mutating the original uploader folder
	rootFolder = bmh.arguments.Uploader.Folder()
	sentinelFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	sentinelUploader, err := internal.ConfigureUploaderToFolder(sentinelFolder)
	if err != nil {
		return fmt.Errorf("failed to configure uploader for sentinel folder: %v", err)
	}
	if err = internal.UploadSentinel(sentinelUploader, mergedSentinel, mergedBackupName); err != nil {
		return fmt.Errorf("failed to upload merged backup sentinel: %v", err)
	}

	metaFileName := RestorePointMetadataFileName(mergedBackupName)
	tracelog.InfoLogger.Printf("Uploading restore point metadata file %s", metaFileName)
	tracelog.InfoLogger.Println(targetRestorePoint)
	if err := internal.UploadDto(sentinelUploader.Folder(), targetRestorePoint, metaFileName); err != nil {
		return fmt.Errorf("upload metadata file for restore point %s: %w", mergedBackupName, err)
	}

	tracelog.InfoLogger.Printf("Successfully created merged backup metadata for: %s", mergedBackupName)
	return nil
}

func (bmh *BackupMergeHandler) cleanupAfterMerge() error {
	tracelog.InfoLogger.Println("Starting post-merge cleanup")
	rootFolder := bmh.arguments.Uploader.Folder()
	tracelog.InfoLogger.Printf(
		"[cleanupAfterMerge] rootFolder path: %s, used storages: %v",
		rootFolder.GetPath(),
		multistorage.UsedStorages(rootFolder),
	)

	// 1) Delete old incremental chain: wal-g gp delete target FIND_FULL <BASE_BACKUP_NAME> --confirm
	if bmh.backupSentinelDto != nil {
		// Use the base backup name (left of "_D_") so FIND_FULL can resolve and delete the entire chain
		baseBackupName := *bmh.backupSentinelDto.IncrementFullName
		tracelog.InfoLogger.Printf("[cleanupAfterMerge] FIND_FULL base backup name: %s", baseBackupName)

		delTargetArgs := DeleteArgs{
			Confirmed: true,
			FindFull:  true,
		}
		delTargetHandler, err := NewDeleteHandler(rootFolder, delTargetArgs)
		if err != nil {
			return fmt.Errorf("failed to create delete handler for target cleanup: %v", err)
		}

		selector, err := internal.NewBackupNameSelector(baseBackupName, true)
		if err != nil {
			return fmt.Errorf("failed to build backup selector for %s: %v", baseBackupName, err)
		}

		// Find target by selector and delete it; fail if not found to match CLI behavior
		target, err := delTargetHandler.FindTargetBySelector(selector)
		if err != nil {
			return fmt.Errorf("failed to find target backup %s for deletion: %v", baseBackupName, err)
		}
		if target == nil {
			return fmt.Errorf("requested backup '%s' was not found", baseBackupName)
		}

		tracelog.InfoLogger.Println("Deleting the segments backups for old chain...")
		if err := delTargetHandler.dispatchDeleteCmd(target, SegDeleteTarget); err != nil {
			return fmt.Errorf("failed to delete the segments backups: %v", err)
		}
		tracelog.InfoLogger.Printf("Finished deleting the segments backups")

		folderFilter := func(name string) bool { return true }
		if err := delTargetHandler.DeleteTarget(target, delTargetArgs.Confirmed, delTargetArgs.FindFull, folderFilter); err != nil {
			return fmt.Errorf("failed to delete target '%s': %v", baseBackupName, err)
		}
		tracelog.InfoLogger.Printf("Deleted old incremental backup chain for target: %s", bmh.arguments.targetBackupName)
	} else {
		tracelog.InfoLogger.Println("Merged backup is full or base backup name is unknown; skipping unused chain deletion")
	}

	// 2) Purge WAL archives and leftover files: wal-g gp delete garbage --confirm
	delGarbageArgs := DeleteArgs{Confirmed: true, Force: true}
	delGarbageHandler, err := NewDeleteHandler(rootFolder, delGarbageArgs)
	if err != nil {
		return fmt.Errorf("failed to create delete handler for garbage cleanup: %v", err)
	}
	if err := delGarbageHandler.HandleDeleteGarbage([]string{}); err != nil {
		return fmt.Errorf("failed to delete garbage after merge: %v", err)
	}
	tracelog.InfoLogger.Printf("Garbage cleanup completed after merge")

	return nil
}
