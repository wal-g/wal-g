package st

import (
	"fmt"
	"math"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/storagetools"
)

const transferShortDescription = "Moves objects from one storage to another (Postgres only)"

// transferCmd represents the transfer command
var transferCmd = &cobra.Command{
	Use:   "transfer prefix --source='source_storage' [--target='target_storage']",
	Short: transferShortDescription,
	Long: "The command is usually used to move objects from a failover storage to the primary one, when it becomes alive. " +
		"By default, objects that exist in both storages are neither overwritten in the target storage nor deleted from the source one.",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := validateFlags()
		if err != nil {
			tracelog.ErrorLogger.FatalError(fmt.Errorf("invalid flags: %w", err))
		}

		cfg := &storagetools.TransferHandlerConfig{
			Prefix:                   args[0],
			Overwrite:                transferOverwrite,
			FailOnFirstErr:           transferFailFast,
			Concurrency:              transferConcurrency,
			MaxFiles:                 adjustMaxFiles(transferMax),
			AppearanceChecks:         transferAppearanceChecks,
			AppearanceChecksInterval: transferAppearanceChecksInterval,
		}

		handler, err := storagetools.NewTransferHandler(transferSourceStorage, targetStorage, cfg)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}

		err = handler.Handle()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
	},
}

var (
	transferSourceStorage            string
	transferOverwrite                bool
	transferFailFast                 bool
	transferConcurrency              int
	transferMax                      int
	transferAppearanceChecks         uint
	transferAppearanceChecksInterval time.Duration
)

func init() {
	transferCmd.Flags().StringVarP(&transferSourceStorage, "source", "s", "",
		"storage name to move files from. Use 'default' to select the primary storage")
	transferCmd.Flags().BoolVarP(&transferOverwrite, "overwrite", "o", false,
		"whether to overwrite already existing files in the target storage and remove them from the source one")
	transferCmd.Flags().BoolVar(&transferFailFast, "fail-fast", false,
		"if this flag is set, any error occurred with transferring a separate file will lead the whole command to stop immediately")
	transferCmd.Flags().IntVarP(&transferConcurrency, "concurrency", "c", 10,
		"number of concurrent workers to move files. Value 1 turns concurrency off")
	transferCmd.Flags().IntVarP(&transferMax, "max", "m", -1,
		"max number of files to move in this run. Negative numbers turn the limit off")
	transferCmd.Flags().UintVar(&transferAppearanceChecks, "appearance-checks", 3,
		"number of times to check if a file is appeared for reading in the target storage after writing it. Value 0 turns checking off")
	transferCmd.Flags().DurationVar(&transferAppearanceChecksInterval, "appearance-checks-interval", time.Second,
		"minimum time interval between performing checks for files to appear in the target storage")

	StorageToolsCmd.AddCommand(transferCmd)
}

func validateFlags() error {
	if transferSourceStorage == "" {
		return fmt.Errorf("source storage must be specified")
	}
	if transferSourceStorage == "all" {
		return fmt.Errorf("an explicit source storage must be specified instead of 'all'")
	}
	if targetStorage == "all" {
		return fmt.Errorf("an explicit target storage must be specified instead of 'all'")
	}
	if transferSourceStorage == targetStorage {
		return fmt.Errorf("source and target storages must be different")
	}
	if transferConcurrency < 1 {
		return fmt.Errorf("concurrency level must be >= 1 (which turns it off)")
	}
	return nil
}

func adjustMaxFiles(max int) int {
	if max < 0 {
		return math.MaxInt
	}
	return max
}
