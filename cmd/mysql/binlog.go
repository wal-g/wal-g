package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"os"
)

const binlogDescription = ""

// binlogPushCmd represents the cron command
var binlogCmd = &cobra.Command{
	Use:   "backup",
	Short: binlogDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		tracelog.ErrorLogger.Println("use subcommand")
		os.Exit(1)
	},
}

func init() {
	Cmd.AddCommand(binlogCmd)
}

