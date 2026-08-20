package redis

import (
	"github.com/spf13/cobra"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
)

var (
	copyBackupName string
	copyFrom       string
	copyTo         string
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a Redis or Valkey backup without transforming payloads",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		redisdb.HandleCopy(command.Context(), copyFrom, copyTo, copyBackupName)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	copyCmd.Flags().StringVarP(&copyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	copyCmd.Flags().StringVarP(&copyFrom, "from", "f", "", "source storage configuration file")
	copyCmd.Flags().StringVarP(&copyTo, "to", "t", "", "destination storage configuration file")
	_ = copyCmd.MarkFlagRequired("from")
	_ = copyCmd.MarkFlagRequired("to")
	cmd.AddCommand(copyCmd)
}
