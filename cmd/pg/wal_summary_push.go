package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

func init() {
	var target string
	command := &cobra.Command{
		Use:   "wal-summary-push summary_filepath",
		Short: "Upload a PostgreSQL WAL summary to wal_sum_005/ (PoC)",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			stores, err := internal.ConfigureMultiStorage(cmd.Context(), true)
			tracelog.ErrorLogger.FatalOnError(err)
			folder := multistorage.SetPolicies(stores.RootFolder(), policies.TakeFirstStorage)
			if target == "" {
				folder, err = multistorage.UseFirstAliveStorage(cmd.Context(), folder)
			} else {
				folder, err = multistorage.UseSpecificStorage(cmd.Context(), target, folder)
			}
			tracelog.ErrorLogger.FatalOnError(err)
			uploader, err := internal.ConfigureUploaderToFolder(folder)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.ErrorLogger.FatalOnError(postgres.HandleWalSummaryPush(cmd.Context(), uploader, args[0]))
		},
	}
	command.Flags().StringVar(&target, "target-storage", "", targetStorageDescription)
	Cmd.AddCommand(command)
}
