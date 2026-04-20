package gp

import (
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/logging"

	"github.com/spf13/cobra"
)

const (
	createRestorePointDescription = "Creates cluster-wide restore point with the specified name"
)

var (
	// createRestorePointCmd represents the createRestorePoint command
	createRestorePointCmd = &cobra.Command{
		Use:   "create-restore-point name",
		Short: createRestorePointDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name := args[0]

			restorePointCreator, err := greenplum.NewRestorePointCreator(name)
			logging.FatalOnError(err)

			restorePointCreator.Create()
		},
	}
)

func init() {
	cmd.AddCommand(createRestorePointCmd)
}
