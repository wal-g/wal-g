package greenplum

import (
	"fmt"
	"path"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type FollowPrimaryHandler struct {
	cluster            *cluster.Cluster
	stopAtRestorePoint string
	timeoutInSeconds   int
}

// nolint:gocritic
func NewFollowPrimaryHandler(
	logsDir string,
	restoreCfgPath, stopAtRestorePoint string,
	timeoutInSeconds int,
) *FollowPrimaryHandler {
	restoreCfg, err := readRestoreConfig(restoreCfgPath)
	tracelog.ErrorLogger.FatalOnError(err)

	initGpLog(logsDir)

	segmentConfigs := make([]cluster.SegConfig, 0)
	for contentID, segRestoreCfg := range restoreCfg.Segments {
		segmentConfigs = append(segmentConfigs, segRestoreCfg.ToSegConfig(contentID))
	}

	globalCluster := cluster.NewCluster(segmentConfigs)
	tracelog.DebugLogger.Printf("cluster %v\n", globalCluster)

	return &FollowPrimaryHandler{
		cluster:            globalCluster,
		stopAtRestorePoint: stopAtRestorePoint,
		timeoutInSeconds:   timeoutInSeconds,
	}
}

func (fh *FollowPrimaryHandler) Follow() {
	tracelog.InfoLogger.Println("Updating recovery.conf on segments and master...")
	err := fh.updateRecoveryConfigs()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Running recovery on segments and master...")
	// Run WAL-G to restore the each segment as a single Postgres instance
	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return fh.buildSegmentStartCommand(contentID)
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("WAL-G output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
}

// updateRecoveryConfigs generates and uploads the correct recovery.conf
// files to each segment instance (including master) so they can recover correctly
// up to the expected restore point
func (fh *FollowPrimaryHandler) updateRecoveryConfigs() error {
	recoveryTarget := fh.stopAtRestorePoint
	tracelog.InfoLogger.Printf("Recovery target is %s", recoveryTarget)
	restoreCfgMaker := NewRecoveryConfigMaker(
		"/usr/bin/wal-g", internal.CfgFile, recoveryTarget, true)

	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Updating recovery.conf on segments and master",
		cluster.ON_SEGMENTS|cluster.EXCLUDE_MIRRORS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			segment := fh.cluster.ByContent[contentID][0]
			pathToRestore := path.Join(segment.DataDir, "recovery.conf")
			fileContents := restoreCfgMaker.Make(contentID)
			cmd := fmt.Sprintf("cat > %s << EOF\n%s\nEOF", pathToRestore, fileContents)
			tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmd)
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to update recovery.conf", func(contentID int) string {
		return fmt.Sprintf("Unable to create recovery.conf on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("Update recovery.conf output (segment %d):\n%s\n", command.Content, command.Stderr)
	}

	return nil
}

// TODO: Unit tests
func (fh *FollowPrimaryHandler) buildSegmentStartCommand(contentID int) string {
	segment := fh.cluster.ByContent[contentID][0]
	cmd := []string{
		"pg_ctl",
		"-w",
		"-c gp_role=utility",
		fmt.Sprintf("-p %d", segment.Port),
		fmt.Sprintf("-t %d", fh.timeoutInSeconds),
		fmt.Sprintf("-D %s", segment.DataDir),
		// forward STDOUT& STDERR to log file
		">>", formatSegmentLogPath(contentID), "2>&1",
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}
