package redis

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
)

var tag string

var backupInfoCmd = &cobra.Command{
	Use:   "backup-info",
	Short: "Prints redis backup info",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)

		backupName := args[0]
		redis.HandleBackupInfo(storage.RootFolder(), backupName, os.Stdout, tag)
	},
}

func init() {
	backupInfoCmd.PersistentFlags().StringVar(&tag, "tag", "", "print specified field value only")
	cmd.AddCommand(backupInfoCmd)
}
