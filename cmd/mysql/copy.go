package mysql

import (
	"github.com/spf13/cobra"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

var (
	uniformCopyBackupName  string
	uniformCopyFrom        string
	uniformCopyTo          string
	uniformCopyWithHistory bool
)

var uniformCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy a MySQL backup and optionally synchronize binlog history",
	Args:  cobra.NoArgs,
	Run: func(command *cobra.Command, _ []string) {
		db.HandleCopy(command.Context(), uniformCopyFrom, uniformCopyTo, uniformCopyBackupName, uniformCopyWithHistory)
	},
	PersistentPreRun: func(*cobra.Command, []string) {},
}

func init() {
	uniformCopyCmd.Flags().StringVarP(&uniformCopyBackupName, "backup-name", "b", "", "copy one backup (or LATEST); empty copies all")
	uniformCopyCmd.Flags().StringVarP(&uniformCopyFrom, "from", "f", "", "source storage configuration file")
	uniformCopyCmd.Flags().StringVarP(&uniformCopyTo, "to", "t", "", "destination storage configuration file")
	uniformCopyCmd.Flags().BoolVarP(
		&uniformCopyWithHistory, "with-history", "w", false,
		"synchronize binlog history through the latest archived entry")
	_ = uniformCopyCmd.MarkFlagRequired("from")
	_ = uniformCopyCmd.MarkFlagRequired("to")
	cmd.AddCommand(uniformCopyCmd)
}
