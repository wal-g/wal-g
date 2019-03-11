package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// walPrefetchCmd represents the walPrefetch command
var walPrefetchCmd = &cobra.Command{
	Use:   "wal-prefetch", // TODO : description
	//ValidArgs: []string{"wal_filename", "prefetch_location"},
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleWALPrefetch(uploader, args[0], args[1])
	},
}

func init() {
	RootCmd.AddCommand(walPrefetchCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// walPrefetchCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// walPrefetchCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
