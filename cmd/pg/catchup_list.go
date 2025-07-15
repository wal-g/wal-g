package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

const (
	catchupListShortDescription = "Prints available incremental backups"
)

var (
	// catchupListCmd represents the catchupList command
	catchupListCmd = &cobra.Command{
		Use:   "catchup-list",
		Short: catchupListShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			storage, err := internal.ConfigureStorage()
			tracelog.ErrorLogger.FatalOnError(err)
			if detail {
				postgres.HandleDetailedBackupList(storage.RootFolder().GetSubFolder(utility.CatchupPath), pretty, json)
			} else {
				internal.HandleDefaultBackupList(storage.RootFolder().GetSubFolder(utility.CatchupPath), pretty, json)
			}
		},
	}
)

func init() {
	Cmd.AddCommand(catchupListCmd)

	catchupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	catchupListCmd.Flags().BoolVar(&json, JSONFlag, false, "Prints output in json format")
	catchupListCmd.Flags().BoolVar(&detail, DetailFlag, false, "Prints extra backup details")
}
