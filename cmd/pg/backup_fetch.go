package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupFetchShortDescription = "Fetches a backup from storage"
	maskFlagDescription         = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
	restoreSpecDescription        = "Path to file containing tablespace restore specification"
	reverseDeltaUnpackDescription = "Unpack delta backups in reverse order (beta feature)"
	skipRedundantTarsDescription  = "Skip tars with no useful data (requires reverse delta unpack)"
)

var fileMask string
var restoreSpec string
var reverseDeltaUnpack bool
var skipRedundantTars bool

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory backup_name",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		var pgFetcher func(folder storage.Folder, backup internal.Backup)
		reverseDeltaUnpack = reverseDeltaUnpack || viper.GetBool(internal.UseReverseUnpackSetting)
		skipRedundantTars = skipRedundantTars || viper.GetBool(internal.SkipRedundantTarsSetting)
		if reverseDeltaUnpack {
			pgFetcher = internal.GetPgFetcherNew(args[0], fileMask, restoreSpec, skipRedundantTars)
		} else {
			pgFetcher = internal.GetPgFetcherOld(args[0], fileMask, restoreSpec)
		}

		internal.HandleBackupFetch(folder, args[1], pgFetcher)
	},
}

func init() {
	backupFetchCmd.Flags().StringVar(&fileMask, "mask", "", maskFlagDescription)
	backupFetchCmd.Flags().StringVar(&restoreSpec, "restore-spec", "", restoreSpecDescription)
	backupFetchCmd.Flags().BoolVar(&reverseDeltaUnpack, "reverse-unpack",
		false, reverseDeltaUnpackDescription)
	backupFetchCmd.Flags().BoolVar(&skipRedundantTars, "skip-redundant-tars",
		false, skipRedundantTarsDescription)
	cmd.AddCommand(backupFetchCmd)
}
