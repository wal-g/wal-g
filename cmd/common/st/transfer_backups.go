package st

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/storagetools/transfer"
)

const backupsShortDescription = "Moves all backups from one storage to another"

// backupsCmd represents the backups command
var backupsCmd = &cobra.Command{
	Use:   "backups [backup_name] --source='source_storage' [--target='target_storage']",
	Short: backupsShortDescription,
	Args:  cobra.RangeArgs(0, 1),
	Run: func(_ *cobra.Command, args []string) {
		var fileLister transfer.FileLister
		if len(args) == 0 {
			fileLister = transfer.NewAllBackupsFileLister(transferOverwrite, transferMaxFiles, adjustMax(transferMaxBackups))
		} else {
			fileLister = transfer.NewSingleBackupFileLister(args[0], transferOverwrite, transferMaxFiles)
		}

		cfg := &transfer.HandlerConfig{
			FailOnFirstErr:           transferFailFast,
			Concurrency:              transferConcurrency,
			AppearanceChecks:         transferAppearanceChecks,
			AppearanceChecksInterval: transferAppearanceChecksInterval,
		}

		handler, err := transfer.NewHandler(transferSourceStorage, targetStorage, fileLister, cfg)
		tracelog.ErrorLogger.FatalOnError(err)

		err = handler.Handle()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

var transferMaxBackups int

func init() {
	backupsCmd.Flags().IntVar(&transferMaxBackups, "max-backups", -1,
		"max number of backups to move in this run. Is ignored if backup_name is specified. Negative numbers turn the limit off")

	transferCmd.AddCommand(backupsCmd)
}
