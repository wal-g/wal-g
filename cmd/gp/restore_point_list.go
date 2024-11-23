package gp

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/utility"
)

const (
	restorePointListShortDescription = "Prints available restore points"
)

var (
	// restorePointListCmd represents the restorePointList command
	restorePointListCmd = &cobra.Command{
		Use:   "restore-point-list",
		Short: restorePointListShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			storage, err := internal.ConfigureStorage()
			tracelog.ErrorLogger.FatalOnError(err)
			greenplum.HandleRestorePointList(storage.RootFolder().GetSubFolder(utility.BaseBackupPath), greenplum.NewGenericMetaInteractor(), pretty, jsonOutput)
		},
	}
)

func init() {
	cmd.AddCommand(restorePointListCmd)

	restorePointListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	restorePointListCmd.Flags().BoolVar(&jsonOutput, JSONFlag, false, "Prints output in json format")
}
