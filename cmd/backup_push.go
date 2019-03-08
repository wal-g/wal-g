package cmd

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

// backupPushCmd represents the backupPush command
var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: "makes backup and uploads it to storage", // TODO
	//ValidArgs: []string{"db_directory"},
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleBackupPush(uploader, args[0])
	},
}

func init() {
	RootCmd.AddCommand(backupPushCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// backupPushCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// backupPushCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
