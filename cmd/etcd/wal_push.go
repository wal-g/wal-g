package etcd

import (
	"context"
	"os"

	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"

	"github.com/wal-g/wal-g/utility"
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
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		tracelog.ErrorLogger.FatalOnError(err)

		dataDir, err := conf.GetRequiredSetting(conf.ETCDMemberDataDirectory)
		tracelog.ErrorLogger.FatalOnError(err)

		err = etcd.HandleWALPush(ctx, uploader, dataDir)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(walPushCmd)
}
