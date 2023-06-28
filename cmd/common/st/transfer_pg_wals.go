package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/utility"
)

const pgWALsShortDescription = "Moves all PostgreSQL WAL files from one storage to another"

// pgWALsCmd represents the pg-wals command
var pgWALsCmd = &cobra.Command{
	Use:   "pg-wals --source='source_storage' [--target='target_storage']",
	Short: pgWALsShortDescription,
	Args:  cobra.NoArgs,
	Run: func(_ *cobra.Command, _ []string) {
		transferFiles(utility.WalPath)
	},
}

func init() {
	transferCmd.AddCommand(pgWALsCmd)
}
