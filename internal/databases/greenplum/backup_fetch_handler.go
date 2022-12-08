package greenplum

import (
	"fmt"
	"path"
	"strings"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupFetchMode string

const (
	DefaultFetchMode BackupFetchMode = "default"
	UnpackFetchMode  BackupFetchMode = "unpack"
	PrepareFetchMode BackupFetchMode = "prepare"
)

func NewBackupFetchMode(mode string) (BackupFetchMode, error) {
	switch mode {
	case string(DefaultFetchMode):
		return DefaultFetchMode, nil
	case string(UnpackFetchMode):
		return UnpackFetchMode, nil
	case string(PrepareFetchMode):
		return PrepareFetchMode, nil
	default:
		return "", errors.Errorf("unknown backup fetch mode: %s", mode)
	}
}

type SegmentRestoreConfig struct {
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
	DataDir  string `json:"data_dir"`
}

// ClusterRestoreConfig is used to describe the restored cluster
type ClusterRestoreConfig struct {
	Segments map[int]SegmentRestoreConfig `json:"segments"`
}

type FetchHandler struct {
	cluster             *cluster.Cluster
	backupIDByContentID map[int]string
	backup              internal.Backup
	contentIDsToFetch   map[int]bool
	fetchMode           BackupFetchMode
	restorePoint        string
}

// nolint:gocritic
func NewFetchHandler(
	backup internal.Backup, sentinel BackupSentinelDto,
	segCfgMaker SegConfigMaker, logsDir string,
	fetchContentIds []int, mode BackupFetchMode,
	restorePoint string,
) *FetchHandler {
	backupIDByContentID := make(map[int]string)
	segmentConfigs := make([]cluster.SegConfig, 0)
	initGpLog(logsDir)

	for _, segMeta := range sentinel.Segments {
		// currently, WAL-G does not restore the mirrors
		if segMeta.Role == Primary {
			// update the segment config from the metadata with the
			// Hostname, Port and DataDir specified in the restore config
			backupIDByContentID[segMeta.ContentID] = segMeta.BackupID
			segmentCfg, err := segCfgMaker.Make(segMeta)
			tracelog.ErrorLogger.FatalOnError(err)

			segmentConfigs = append(segmentConfigs, segmentCfg)
		} else {
			tracelog.WarningLogger.Printf(
				"Skipping non-primary segment: DatabaseID %d, Hostname %s, DataDir: %s\n", segMeta.DatabaseID, segMeta.Hostname, segMeta.DataDir)
		}
	}

	globalCluster := cluster.NewCluster(segmentConfigs)
	tracelog.DebugLogger.Printf("cluster %v\n", globalCluster)

	return &FetchHandler{
		cluster:             globalCluster,
		backupIDByContentID: backupIDByContentID,
		backup:              backup,
		contentIDsToFetch:   prepareContentIDsToFetch(fetchContentIds, segmentConfigs),
		fetchMode:           mode,
		restorePoint:        restorePoint,
	}
}

// TODO: Unit tests
// prepareContentIDsToFetch returns a set containing the IDs of segments to be fetched
func prepareContentIDsToFetch(fetchContentIds []int, segmentConfigs []cluster.SegConfig) map[int]bool {
	contentIDsToFetch := make(map[int]bool)

	// if user set the specific content IDs, use only them, otherwise fetch all
	if len(fetchContentIds) > 0 {
		for _, id := range fetchContentIds {
			contentIDsToFetch[id] = true
		}
	} else {
		for _, cfg := range segmentConfigs {
			contentIDsToFetch[cfg.ContentID] = true
		}
	}

	return contentIDsToFetch
}

func (fh *FetchHandler) Fetch() error {
	if fh.fetchMode == DefaultFetchMode || fh.fetchMode == UnpackFetchMode {
		fh.Unpack()
	}

	if fh.fetchMode == DefaultFetchMode || fh.fetchMode == PrepareFetchMode {
		return fh.Prepare()
	}

	return nil
}

func (fh *FetchHandler) Unpack() {
	tracelog.InfoLogger.Println("[Unpack] Running wal-g on segments and master...")

	// Run WAL-G to restore the each segment as a single Postgres instance
	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return fh.buildFetchCommand(contentID)
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("[Unpack] WAL-G output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
}

func (fh *FetchHandler) Prepare() error {
	tracelog.InfoLogger.Println("[Prepare] Updating pg_hba configs on segments...")
	err := fh.createPgHbaOnSegments()
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Println("[Prepare] Creating recovery.conf files...")
	return fh.createRecoveryConfigs()
}

// createPgHbaOnSegments generates and uploads the correct pg_hba.conf
// files to each segment instance (except the master) so they can communicate correctly
func (fh *FetchHandler) createPgHbaOnSegments() error {
	pgHbaMaker := NewPgHbaMaker(fh.cluster.ByContent)
	fileContents, err := pgHbaMaker.Make()
	if err != nil {
		return err
	}

	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Updating pg_hba on segments",
		cluster.ON_SEGMENTS|cluster.EXCLUDE_MIRRORS,
		func(contentID int) string {
			if !fh.contentIDsToFetch[contentID] {
				return newSkippedSegmentMsg(contentID)
			}

			segment := fh.cluster.ByContent[contentID][0]
			pathToHba := path.Join(segment.DataDir, "pg_hba.conf")

			cmd := fmt.Sprintf("cat > %s << EOF\n%s\nEOF", pathToHba, fileContents)
			tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmd)
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to update pg_hba", func(contentID int) string {
		return fmt.Sprintf("Unable to update pg_hba on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("Update pg_hba output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
	return nil
}

// createRecoveryConfigs generates and uploads the correct recovery.conf
// files to each segment instance (including master) so they can recover correctly
// during the database startup
func (fh *FetchHandler) createRecoveryConfigs() error {
	recoveryTarget := fh.backup.Name
	if fh.restorePoint != "" {
		recoveryTarget = fh.restorePoint
	}
	tracelog.InfoLogger.Printf("Recovery target is %s", recoveryTarget)
	restoreCfgMaker := NewRecoveryConfigMaker("/usr/bin/wal-g", internal.CfgFile, recoveryTarget)

	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Creating recovery.conf on segments and master",
		cluster.ON_SEGMENTS|cluster.EXCLUDE_MIRRORS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			if !fh.contentIDsToFetch[contentID] {
				return newSkippedSegmentMsg(contentID)
			}

			segment := fh.cluster.ByContent[contentID][0]
			pathToRestore := path.Join(segment.DataDir, "recovery.conf")
			fileContents := restoreCfgMaker.Make(contentID)
			cmd := fmt.Sprintf("cat > %s << EOF\n%s\nEOF", pathToRestore, fileContents)
			tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmd)
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to create recovery.conf", func(contentID int) string {
		return fmt.Sprintf("Unable to create recovery.conf on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("Create recovery.conf output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
	return nil
}

// TODO: Unit tests
// buildFetchCommand creates the WAL-G command to restore the segment with
// the provided contentID
func (fh *FetchHandler) buildFetchCommand(contentID int) string {
	if !fh.contentIDsToFetch[contentID] {
		return newSkippedSegmentMsg(contentID)
	}

	segment := fh.cluster.ByContent[contentID][0]
	backupID, ok := fh.backupIDByContentID[contentID]
	if !ok {
		// this should never happen
		tracelog.ErrorLogger.Fatalf("Failed to load backup id by content id %d", contentID)
	}

	segUserData := NewSegmentUserDataFromID(backupID)
	cmd := []string{
		fmt.Sprintf("PGPORT=%d", segment.Port),
		"wal-g seg-backup-fetch",
		fmt.Sprint(segment.DataDir),
		fmt.Sprintf("--content-id=%d", segment.ContentID),
		fmt.Sprintf("--target-user-data=%s", segUserData.QuotedString()),
		fmt.Sprintf("--config=%s", internal.CfgFile),
		// forward STDOUT& STDERR to log file
		">>", formatSegmentLogPath(contentID), "2>&1",
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

func NewGreenplumBackupFetcher(restoreCfgPath string, inPlaceRestore bool, logsDir string,
	fetchContentIds []int, mode BackupFetchMode, restorePoint string,
) func(folder storage.Folder, backup internal.Backup) {
	return func(folder storage.Folder, backup internal.Backup) {
		tracelog.InfoLogger.Printf("Starting backup-fetch for %s", backup.Name)
		if restorePoint != "" {
			tracelog.ErrorLogger.FatalOnError(ValidateMatch(folder, backup.Name, restorePoint))
		}
		var sentinel BackupSentinelDto
		err := backup.FetchSentinel(&sentinel)
		tracelog.ErrorLogger.FatalOnError(err)

		segCfgMaker, err := NewSegConfigMaker(restoreCfgPath, inPlaceRestore)
		tracelog.ErrorLogger.FatalOnError(err)

		err = NewFetchHandler(backup, sentinel, segCfgMaker, logsDir, fetchContentIds, mode, restorePoint).Fetch()
		tracelog.ErrorLogger.FatalOnError(err)
	}
}

func newSkippedSegmentMsg(contentID int) string {
	return fmt.Sprintf("echo 'skipping contentID %d: disabled in config'", contentID)
}
