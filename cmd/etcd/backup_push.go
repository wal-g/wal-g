package etcd

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupPushShortDescription = "Creates new backup and pushes it to storage"

	addUserDataFlag = "add-user-data"
	permanentFlag   = "permanent"

	permanentShorthand = "p"
)

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: backupPushShortDescription,
	PreRun: func(cmd *cobra.Command, args []string) {
		conf.RequiredSettings[conf.NameStreamCreateCmd] = true
		err := internal.AssertRequiredSettingsSet()
		logging.FatalOnError(err)
	},
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		uploader, err := internal.ConfigureUploader()
		logging.FatalOnError(err)
		uploader.ChangeDirectory(utility.BaseBackupPath)

		backupCmd, err := internal.GetCommandSetting(conf.NameStreamCreateCmd)
		logging.FatalOnError(err)

		if userDataRaw == "" {
			userDataRaw = viper.GetString(conf.SentinelUserDataSetting)
		}

		etcd.HandleBackupPush(uploader, backupCmd, permanent, userDataRaw)
	},
}

var (
	userDataRaw = ""
	permanent   = false
)

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	backupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
}
