package mongo

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/common"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	mongodb "github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

var (
	copyBackupName  string
	copyFrom        string
	copyTo          string
	copyWithHistory bool
)

var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a MongoDB backup and optionally synchronize oplog history",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		mongodb.HandleCopy(command.Context(), copyFrom, copyTo, copyBackupName, copyWithHistory)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	copyCmd.Flags().StringVarP(&copyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	copyCmd.Flags().StringVarP(&copyFrom, "from", "f", "", "source storage configuration file")
	copyCmd.Flags().StringVarP(&copyTo, "to", "t", "", "destination storage configuration file")
	copyCmd.Flags().BoolVarP(&copyWithHistory, "with-history", "w", false, "synchronize oplog history through the latest archived entry")
	_ = copyCmd.MarkFlagRequired("from")
	common.AddCopyOutputFlags(copyCmd, func(command *cobra.Command, from storage.Folder) (*copyutil.Plan, error) {
		return mongodb.BuildCopyPlan(command.Context(), from, nil, copyBackupName, copyWithHistory)
	})
	cmd.AddCommand(copyCmd)
}
