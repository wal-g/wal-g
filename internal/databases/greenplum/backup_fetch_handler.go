package greenplum

import (
	"fmt"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

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
}

func NewFetchHandler(sentinel BackupSentinelDto, restoreCfg ClusterRestoreConfig) *FetchHandler {
	backupIDByContentID := make(map[int]string)
	segmentConfigs := make([]cluster.SegConfig, 0)

	for _, segMeta := range sentinel.Segments {
		if segMeta.Role == Primary {
			backupIDByContentID[segMeta.ContentID] = segMeta.BackupID
			segmentCfg := segMeta.ToSegConfig()
			segRestoreCfg, ok := restoreCfg.Segments[segMeta.ContentID]
			if !ok {
				tracelog.ErrorLogger.Fatalf(
					"Could not find content ID %d in the provided restore configuration", segMeta.ContentID)
			}
			segmentCfg.Hostname = segRestoreCfg.Hostname
			segmentCfg.Port = segRestoreCfg.Port
			segmentCfg.DataDir = segRestoreCfg.DataDir
			segmentConfigs = append(segmentConfigs, segmentCfg)
		} else {
			// currently, WAL-G does not restore the mirrors
			tracelog.WarningLogger.Printf(
				"Skipping non-primary segment: DatabaseID %d, Hostname %s, DataDir: %s\n", segMeta.DatabaseID, segMeta.Hostname, segMeta.DataDir)
		}
	}
	gplog.InitializeLogging("wal-g", "")

	globalCluster := cluster.NewCluster(segmentConfigs)
	tracelog.DebugLogger.Printf("cluster %v\n", globalCluster)

	return &FetchHandler{
		cluster:             globalCluster,
		backupIDByContentID: backupIDByContentID,
	}
}

func (fh *FetchHandler) Fetch() error {
	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return fh.buildCommand(contentID)
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("WAL-G output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
	return nil
}

func (fh *FetchHandler) buildCommand(contentID int) string {
	segment := fh.cluster.ByContent[contentID][0]
	backupID, ok := fh.backupIDByContentID[contentID]
	if !ok {
		// this should never happen
		tracelog.ErrorLogger.Fatalf("Failed to load backup id by content id")
	}

	segUserData := NewSegmentUserDataFromID(backupID)
	cmd := []string{
		"WALG_LOG_LEVEL=DEVEL",
		fmt.Sprintf("PGPORT=%d", segment.Port),
		"wal-g pg",
		fmt.Sprintf("backup-fetch %s", segment.DataDir),
		fmt.Sprintf("--walg-storage-prefix=%d", segment.ContentID),
		fmt.Sprintf("--target-user-data=%s", segUserData.QuotedString()),
		fmt.Sprintf("--config=%s", internal.CfgFile),
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

func NewGreenplumBackupFetcher(restoreCfg ClusterRestoreConfig) func(folder storage.Folder, backup internal.Backup) {
	return func(folder storage.Folder, backup internal.Backup) {
		var sentinel BackupSentinelDto
		err := backup.FetchSentinel(&sentinel)
		tracelog.ErrorLogger.FatalOnError(err)

		err = NewFetchHandler(sentinel, restoreCfg).Fetch()
		tracelog.ErrorLogger.FatalOnError(err)
	}
}
