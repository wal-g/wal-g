package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// backupFetchCmd represents the backupFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch",
	Short: "fetches a backup from storage", // TODO
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleBackupFetch(folder, args[0], args[1])
	},
}

func init() {
	RootCmd.AddCommand(backupFetchCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// backupFetchCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// backupFetchCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
