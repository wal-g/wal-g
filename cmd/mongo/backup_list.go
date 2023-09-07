package mongo

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/common"
)

const (
	backupListShortDescription = "Prints available backups"
	PrettyFlag                 = "pretty"
	JSONFlag                   = "json"
	DetailFlag                 = "detail"
)

var (
	jsonFormat  = false
	prettyPrint = false
	detail      = false
)

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: backupListShortDescription, // TODO : improve description
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		backupFolder, err := common.GetBackupFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		if detail {
			err := mongo.HandleDetailedBackupList(backupFolder, os.Stdout, prettyPrint, jsonFormat)
			tracelog.ErrorLogger.FatalOnError(err)
		} else {
			internal.HandleDefaultBackupList(backupFolder, prettyPrint, jsonFormat)
		}
	},
}

func init() {
	cmd.AddCommand(backupListCmd)

	backupListCmd.Flags().BoolVar(&prettyPrint, PrettyFlag, false, "Prints more readable output")
	backupListCmd.Flags().BoolVar(&jsonFormat, JSONFlag, false, "Prints output in json format")
	// shorthand "v" is required for backward compatibility
	backupListCmd.Flags().BoolVarP(&detail, DetailFlag, "v", false, "Prints extra backup details")
}
