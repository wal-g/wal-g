package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/common"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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
	common.AddCopyOutputFlags(uniformCopyCmd, func(command *cobra.Command, from storage.Folder) (*copyutil.Plan, error) {
		return db.BuildCopyPlan(command.Context(), from, nil, uniformCopyBackupName, uniformCopyWithHistory, "")
	})
	cmd.AddCommand(uniformCopyCmd)
}
