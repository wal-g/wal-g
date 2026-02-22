package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/internal/storagetools/transfer"
)

const filesShortDescription = "Moves all files by a prefix from one storage to another without any special treatment"

// filesCmd represents the files command
var filesCmd = &cobra.Command{
	Use:   "files prefix --source='source_storage' [--target='target_storage']",
	Short: filesShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		transferFiles(args[0])
	},
}

func transferFiles(prefix string) {
	separateFileLister := transfer.NewRegularFileLister(prefix, transferOverwrite, int(transferMaxFiles))

	cfg := &transfer.HandlerConfig{
		PreserveInSource:         transferPreserveInSource,
		FailOnFirstErr:           transferFailFast,
		Concurrency:              transferConcurrency,
		AppearanceChecks:         transferAppearanceChecks,
		AppearanceChecksInterval: transferAppearanceChecksInterval,
	}

	handler, err := transfer.NewHandler(transferSourceStorage, targetStorage, separateFileLister, cfg)
	logging.FatalOnError(err)

	err = handler.Handle()
	logging.FatalOnError(err)
}

func init() {
	transferCmd.AddCommand(filesCmd)
}
