package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

var targetIncrementalBackupName string

const (
	backupMergeShortDescription            = "Create a single backup from delta backups and put it in storage"
	targetIncrementalBackupNameDescription = "Name of the target delta backup relative to which the base backup should be generated"
)

var backupMergeCmd = &cobra.Command{
	Use:   "backup-merge backup_name",
	Short: backupMergeShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// TODO checks args (backup name should be exists and name must be as delta backup)
		targetBackupName := args[0]
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		// TODO move function chooseTarBallComposer() to some tarBalComposerProvider.go (create it)
		composer := chooseTarBallComposer()

		mergeHandler, err := postgres.NewBackupMergeHandler(targetBackupName, folder, composer)
		tracelog.ErrorLogger.FatalOnError(err)

		mergeHandler.HandleBackupMerge()
		tracelog.InfoLogger.Println("DONE")
	},
}

func init() {
	// TODO add flags as backup-fetch
	backupMergeCmd.Flags().StringVar(&targetIncrementalBackupName, "target-backup-name", "",
		targetIncrementalBackupNameDescription)

	Cmd.AddCommand(backupMergeCmd)
}
