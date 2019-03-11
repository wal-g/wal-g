package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push",
	Short: "uploads a WAL file to storage", // TODO
	//ValidArgs: []string{"wal_filepath"},
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleWALPush(uploader, args[0])
	},
}

func init() {
	RootCmd.AddCommand(walPushCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// walPushCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// walPushCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
