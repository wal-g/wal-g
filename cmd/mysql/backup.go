package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"os"
)

const backupDescription = ""

// binlogPushCmd represents the cron command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: backupDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		tracelog.ErrorLogger.Println("use subcommand")
		os.Exit(1)
	},
}

func init() {
	Cmd.AddCommand(backupCmd)
}

