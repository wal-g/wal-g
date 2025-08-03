package greenplum

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"path"
)

type ActionHandler struct {
	cluster *cluster.Cluster
}

const actionCmd = "sed -i '/^recovery_target_action = /d' %s && echo 'recovery_target_action = %s' >> %s"

// nolint:gocritic
func NewActionHandler(logsDir string, restoreCfgPath string) *ActionHandler {
	restoreCfg, err := readRestoreConfig(restoreCfgPath)
	tracelog.ErrorLogger.FatalOnError(err)

	initGpLog(logsDir)

	segmentConfigs := make([]cluster.SegConfig, 0)
	for contentID, segRestoreCfg := range restoreCfg.Segments {
		segmentConfigs = append(segmentConfigs, segRestoreCfg.ToSegConfig(contentID))
	}

	globalCluster := cluster.NewCluster(segmentConfigs)
	tracelog.DebugLogger.Printf("cluster %v\n", globalCluster)

	return &ActionHandler{
		cluster: globalCluster,
	}
}

func (fh *ActionHandler) UpdateAction(action string) {
	tracelog.InfoLogger.Printf("Updating recovery.conf recovery_target_action %s on segments and master...", action)
	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Updating recovery.conf on segments and master",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			segment := fh.cluster.ByContent[contentID][0]
			pathToRestore := path.Join(segment.DataDir, viper.GetString(conf.GPRelativeRecoveryConfPath))
			cmd := fmt.Sprintf(actionCmd, pathToRestore, action, pathToRestore)
			tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmd)
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to update recovery_target_action", func(contentID int) string {
		return fmt.Sprintf("Unable to create recovery.conf on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands {
		tracelog.DebugLogger.Printf("Update recovery.conf output (segment %d):\n%s\n", command.Content, command.Stderr)
	}
}
