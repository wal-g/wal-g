package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	BackupFetchShortDescription = "Fetches a backup from storage"
	MaskFlagDescription         = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
	RestoreSpecDescription = "Path to file containing tablespace restore specification"
	ReverseDeltaUnpackDescription = "Unpack delta backups in reverse order (testing)"
)

var fileMask string
var restoreSpec string
var reverseDeltaUnpack bool

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory backup_name",
	Short: BackupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		var pgFetcher func(folder storage.Folder, backup internal.Backup)
		if reverseDeltaUnpack {
			pgFetcher = internal.GetPgFetcherNew(args[0], fileMask, restoreSpec)
		} else {
			pgFetcher = internal.GetPgFetcherOld(args[0], fileMask, restoreSpec)
		}

		internal.HandleBackupFetch(folder, args[1], pgFetcher)
	},
}

func init() {
	backupFetchCmd.Flags().StringVar(&fileMask, "mask", "", MaskFlagDescription)
	backupFetchCmd.Flags().StringVar(&restoreSpec, "restore-spec", "", RestoreSpecDescription)
	backupFetchCmd.Flags().BoolVar(&reverseDeltaUnpack, "reverse-unpack",
		false, ReverseDeltaUnpackDescription)
	Cmd.AddCommand(backupFetchCmd)
}
