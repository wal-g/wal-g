package loader

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload"
	"github.com/wal-g/wal-g/tests_func/mongodb/mongoload/internal"
)

var ammoFile string

var Cmd = &cobra.Command{
	Use:   "TODO", // TODO: fill use
	Short: "Load tool",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		client, err := internal.NewMongoClient(cmd.Context(), "mongodb://localhost:27018") // TODO: get from environ
		tracelog.ErrorLogger.FatalOnError(err)

		var input io.Reader
		input = os.Stdin
		if ammoFile != "" {
			file, err := os.Open(ammoFile)
			tracelog.ErrorLogger.FatalOnError(err)
			input = file
			defer func() { _ = file.Close() }()
		}

		stat, err := mongoload.HandleLoad(cmd.Context(), input, client, 1)
		tracelog.ErrorLogger.FatalOnError(err)

		err = internal.PrintStat(stat, os.Stdout)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func Execute() {
	Cmd.Flags().StringVarP(&ammoFile, "file", "f", "", "Path to ammo file")

	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
