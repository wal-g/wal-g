package redis

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/common"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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
	common.AddCopyOutputFlags(copyCmd, func(command *cobra.Command, from storage.Folder) (*copyutil.Plan, error) {
		return redisdb.BuildCopyPlan(command.Context(), from, nil, copyBackupName)
	})
	cmd.AddCommand(copyCmd)
}
