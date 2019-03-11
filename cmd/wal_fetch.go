package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch",
	Short: "fetches a WAL file from storage", // TODO
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		internal.HandleWALFetch(folder, args[0], args[1], true)
	},
}

func init() {
	RootCmd.AddCommand(walFetchCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// walFetchCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// walFetchCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
