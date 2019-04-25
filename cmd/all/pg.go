package all

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/pg"
)

const PgShortDescription = "Set of commands for PostgreSQL"

var pgCmd = &cobra.Command{
	Use:     "pg",
	Short:   PgShortDescription, // TODO : improve short and long descriptions
}

func init() {
	pgCmd.AddCommand(pg.BackupFetchCmd)
	pgCmd.AddCommand(pg.BackupListCmd)
	pgCmd.AddCommand(pg.BackupPushCmd)
	pgCmd.AddCommand(pg.DeleteCmd)
	pgCmd.AddCommand(pg.WalFetchCmd)
	pgCmd.AddCommand(pg.WalPrefetchCmd)
	pgCmd.AddCommand(pg.WalPushCmd)

	RootCmd.AddCommand(pgCmd)
}
