package etcd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"
)

const (
	walPushShortDescribtion = "Fetches wals and pushes to storage"
)

var walPushCmd = &cobra.Command{
	Use:   "wal-push",
	Short: walPushShortDescribtion,
	Args:  cobra.NoArgs,
	PreRun: func(cmd *cobra.Command, args []string) {
		conf.RequiredSettings[conf.ETCDMemberDataDirectory] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		dataDir, err := conf.GetRequiredSetting(conf.ETCDMemberDataDirectory)
		tracelog.ErrorLogger.FatalOnError(err)

		err = etcd.HandleWALPush(cmd.Context(), uploader, dataDir)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(walPushCmd)
}
