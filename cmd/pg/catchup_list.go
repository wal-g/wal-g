package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
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
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			if pretty || json || detail {
				internal.HandleBackupListWithFlagsAndTarget(folder, pretty, json, detail, utility.CatchupPath)
			} else {
				internal.DefaultHandleBackupListWithTarget(folder, utility.CatchupPath)
			}
		},
	}
)

func init() {
	cmd.AddCommand(catchupListCmd)

	catchupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	catchupListCmd.Flags().BoolVar(&json, JSONFlag, false, "Prints output in json format")
	catchupListCmd.Flags().BoolVar(&detail, DetailFlag, false, "Prints extra backup details")
}
