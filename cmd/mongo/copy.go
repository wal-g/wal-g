package mongo

import (
	"github.com/spf13/cobra"
	mongodb "github.com/wal-g/wal-g/internal/databases/mongo"
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
	_ = copyCmd.MarkFlagRequired("to")
	cmd.AddCommand(copyCmd)
}
