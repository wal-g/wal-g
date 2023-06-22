package st

import (
	"fmt"
	"math"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

const transferShortDescription = "Moves objects from one storage to another (Postgres only)"

var transferCmd = &cobra.Command{
	Use:   "transfer",
	Short: transferShortDescription,
	Long: "The command allows to move objects between storages. It's usually used to sync the primary storage with " +
		"a failover, when it becomes alive. By default, objects that exist in both storages are neither overwritten " +
		"in the target storage nor deleted from the source one. (Postgres only)",
	PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
		err := validateCommonFlags()
		if err != nil {
			tracelog.ErrorLogger.FatalError(fmt.Errorf("invalid flags: %w", err))
		}
		transferMaxFiles = adjustMax(transferMaxFiles)
		return nil
	},
}

var (
	transferSourceStorage            string
	transferOverwrite                bool
	transferFailFast                 bool
	transferConcurrency              int
	transferMaxFiles                 int
	transferAppearanceChecks         uint
	transferAppearanceChecksInterval time.Duration
)

func init() {
	transferCmd.PersistentFlags().StringVarP(&transferSourceStorage, "source", "s", "",
		"storage name to move files from. Use 'default' to select the primary storage")
	transferCmd.PersistentFlags().BoolVarP(&transferOverwrite, "overwrite", "o", false,
		"whether to overwrite already existing files in the target storage and remove them from the source one")
	transferCmd.PersistentFlags().BoolVar(&transferFailFast, "fail-fast", false,
		"if this flag is set, any error occurred with transferring a separate file will lead the whole command to stop immediately")
	transferCmd.PersistentFlags().IntVarP(&transferConcurrency, "concurrency", "c", 10,
		"number of concurrent workers to move files. Value 1 turns concurrency off")
	transferCmd.PersistentFlags().IntVarP(&transferMaxFiles, "max-files", "m", -1,
		"max number of files to move in this run. Negative numbers turn the limit off")
	transferCmd.PersistentFlags().UintVar(&transferAppearanceChecks, "appearance-checks", 3,
		"number of times to check if a file is appeared for reading in the target storage after writing it. Value 0 turns checking off")
	transferCmd.PersistentFlags().DurationVar(&transferAppearanceChecksInterval, "appearance-checks-interval", time.Second,
		"minimum time interval between performing checks for files to appear in the target storage")

	StorageToolsCmd.AddCommand(transferCmd)
}

func validateCommonFlags() error {
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

func adjustMax(max int) int {
	if max < 0 {
		return math.MaxInt
	}
	return max
}
