package greenplum

import (
	"fmt"
	"log/slog"
	"path"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/spf13/viper"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/logging"
)

type ActionHandler struct {
	cluster *cluster.Cluster
}

const actionCmd = "sed -i '/^recovery_target_action = /d' %s && echo 'recovery_target_action = %s' >> %s"

// nolint:gocritic
func NewActionHandler(logsDir string, restoreCfgPath string) *ActionHandler {
	restoreCfg, err := readRestoreConfig(restoreCfgPath)
	logging.FatalOnError(err)

	initGpLog(logsDir)

	segmentConfigs := make([]cluster.SegConfig, 0)
	for contentID, segRestoreCfg := range restoreCfg.Segments {
		segmentConfigs = append(segmentConfigs, segRestoreCfg.ToSegConfig(contentID))
	}

	globalCluster := cluster.NewCluster(segmentConfigs)
	slog.Debug(fmt.Sprintf("cluster %v\n", globalCluster))

	return &ActionHandler{
		cluster: globalCluster,
	}
}

func (fh *ActionHandler) UpdateAction(action string) {
	slog.Info(fmt.Sprintf("Updating recovery.conf recovery_target_action %s on segments and master...", action))
	remoteOutput := fh.cluster.GenerateAndExecuteCommand("Updating recovery.conf on segments and master",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			segment := fh.cluster.ByContent[contentID][0]
			pathToRestore := path.Join(segment.DataDir, viper.GetString(conf.GPRelativeRecoveryConfPath))
			cmd := fmt.Sprintf(actionCmd, pathToRestore, action, pathToRestore)
			slog.Debug(fmt.Sprintf("Command to run on segment %d: %s", contentID, cmd))
			return cmd
		})

	fh.cluster.CheckClusterError(remoteOutput, "Unable to update recovery_target_action", func(contentID int) string {
		return fmt.Sprintf("Unable to create recovery.conf on segment %d", contentID)
	})

	for _, command := range remoteOutput.Commands { //nolint:gocritic // rangeValCopy
		slog.Debug(fmt.Sprintf("Update recovery.conf output (segment %d):\n%s\n", command.Content, command.Stderr))
	}
}
