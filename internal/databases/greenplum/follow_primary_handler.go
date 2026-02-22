package greenplum

import (
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"

	"github.com/spf13/viper"

	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/wal-g/tracelog"
)

type FollowPrimaryHandler struct {
	cluster            *cluster.Cluster
	stopAtRestorePoint string
	timeoutInSeconds   int
}

const LATEST = "LATEST"
const WalFolder = utility.SegmentsPath + "/seg%d/" + utility.WalPath

// nolint:gocritic
func NewFollowPrimaryHandler(
	folder storage.Folder,
	logsDir string,
	restoreCfgPath, stopAtRestorePoint string,
	timeoutInSeconds int,
) *FollowPrimaryHandler {
	restoreCfg, err := readRestoreConfig(restoreCfgPath)
	logging.FatalOnError(err)

	initGpLog(logsDir)

	segmentConfigs := make([]cluster.SegConfig, 0)
	for contentID, segRestoreCfg := range restoreCfg.Segments {
		segmentConfigs = append(segmentConfigs, segRestoreCfg.ToSegConfig(contentID))
	}

	globalCluster := cluster.NewCluster(segmentConfigs)
	slog.Debug(fmt.Sprintf("cluster %v\n", globalCluster))

	if stopAtRestorePoint == LATEST {
		restorePoints, err := GetRestorePoints(folder.GetSubFolder(utility.BaseBackupPath))
		if _, ok := err.(NoRestorePointsFoundError); ok {
			err = nil
		}
		tracelog.ErrorLogger.FatalfOnError("Get restore points from folder: %v", err)
		sort.Slice(restorePoints, func(i, j int) bool {
			return restorePoints[i].Time.After(restorePoints[j].Time)
		})
		stopAtRestorePoint = restorePoints[0].Name
		slog.Info(fmt.Sprintf("Selected latest restore point: %s", stopAtRestorePoint))
	}

	FatalIfWalLogMissing(stopAtRestorePoint, folder)

	return &FollowPrimaryHandler{
		cluster:            globalCluster,
		stopAtRestorePoint: stopAtRestorePoint,
		timeoutInSeconds:   timeoutInSeconds,
	}
}

func FatalIfWalLogMissing(restorePoint string, folder storage.Folder) {
	metadata, err := FetchRestorePointMetadata(folder, restorePoint)
	if err != nil {
		logging.FatalOnError(err)
	}

	var foundCnt int
outer:
	for seg, lsn := range metadata.LsnBySegment {
		LSN, err := postgres.ParseLSN(lsn)
		if err != nil {
			logging.FatalOnError(err)
		}
		walSegmentNo := postgres.NewWalSegmentNo(LSN)

		subfolder := folder.GetSubFolder(fmt.Sprintf(WalFolder, seg))
		folderObjects, _, err := subfolder.ListFolder()
		if err != nil {
			logging.FatalOnError(err)
		}

		// WAL file example: "000000010000000000000003.lz4" -> base name is "000000010000000000000003"
		walName := walSegmentNo.GetFilename(metadata.TimeLine)
		for _, obj := range folderObjects {
			if strings.HasPrefix(obj.GetName(), walName) {
				foundCnt++
				continue outer
			}
		}
		slog.Warn(fmt.Sprintf("WAL file was not found for segment %v (WAL name: %v)", seg, walName))
	}

	if foundCnt < len(metadata.LsnBySegment) {
		tracelog.ErrorLogger.Fatalln("WAL file was not uploaded for all segments and master")
	}
}

func (fh *FollowPrimaryHandler) Follow() {
	tracelog.InfoLogger.Println("Updating recovery.conf on segments and master...")
	fh.updateRecoveryConfigs()
	fh.applyXLogInCluster()
}

func (fh *FollowPrimaryHandler) applyXLogInCluster() {
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

	for _, command := range remoteOutput.Commands { //nolint:gocritic // rangeValCopy
		slog.Debug(fmt.Sprintf("WAL-G output (segment %d):\n%s\n", command.Content, command.Stderr))
	}
}

// updateRecoveryConfigs generates and uploads the correct recovery.conf
// files to each segment instance (including master) so they can recover correctly
// up to the expected restore point
func (fh *FollowPrimaryHandler) updateRecoveryConfigs() {
	recoveryTarget := fh.stopAtRestorePoint
	slog.Info(fmt.Sprintf("Recovery target is %s", recoveryTarget))
	restoreCfgMaker := NewRecoveryConfigMaker(
		"wal-g", conf.CfgFile, recoveryTarget, true)

	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Updating recovery.conf on segments and master",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			segment := fh.cluster.ByContent[contentID][0]
			pathToRestore := path.Join(segment.DataDir, viper.GetString(conf.GPRelativeRecoveryConfPath))
			// For this feature, we expect Cloudberry / Greenplum 6.25+ (in this version some patches from 9.5 were backported)
			fileContents := restoreCfgMaker.Make(contentID, 90500)
			cmd := fmt.Sprintf("cat > %s << EOF\n%s\nEOF", pathToRestore, fileContents)
			slog.Debug(fmt.Sprintf("Command to run on segment %d: %s", contentID, cmd))
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to update recovery.conf", func(contentID int) string {
		return fmt.Sprintf("Unable to create recovery.conf on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands { //nolint:gocritic // rangeValCopy
		slog.Debug(fmt.Sprintf("Update recovery.conf output (segment %d):\n%s\n", command.Content, command.Stderr))
	}
}

func (fh *FollowPrimaryHandler) buildSegmentStartCommand(contentID int) string {
	segment := fh.cluster.ByContent[contentID][0]
	pgCtlPath := "pg_ctl"
	if viper.IsSet(conf.GPHome) {
		pgCtlPath = path.Join(viper.GetString(conf.GPHome), "bin", "pg_ctl")
	}
	cmd := []string{
		pgCtlPath,
		fmt.Sprintf("-t %d", fh.timeoutInSeconds),
		fmt.Sprintf("-D %s", segment.DataDir),
		"start",
		// forward STDOUT& STDERR to log file
		">>", formatSegmentLogPath(contentID), "2>&1",
	}

	cmdLine := strings.Join(cmd, " ")
	slog.Debug(fmt.Sprintf("Command to run on segment %d: %s", contentID, cmdLine))
	return cmdLine
}
