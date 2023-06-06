package gp

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	segBackupFetchShortDescription = "Fetches a segment backup from storage"
	maskFlagDescription            = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
	restoreSpecDescription = "Path to file containing tablespace restore specification"
)

var fileMask string
var restoreSpec string
var targetUserData string

// segBackupFetchCmd is a subcommand to fetch a backup of a single segment.
// It is called remotely by a backup-fetch command from the master host
var segBackupFetchCmd = &cobra.Command{
	Use:   "seg-backup-fetch destination_directory [backup_name | --target-user-data <data>] --content-id=[content_id]",
	Short: segBackupFetchShortDescription, // TODO : improve description
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		if targetUserData == "" {
			targetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}

		greenplum.SetSegmentStoragePrefix(contentID)

		targetBackupSelector, err := createTargetFetchSegBackupSelector(cmd, args, fetchTargetUserData)
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		reverseDeltaUnpack := viper.GetBool(internal.UseReverseUnpackSetting)
		skipRedundantTars := viper.GetBool(internal.SkipRedundantTarsSetting)

		if reverseDeltaUnpack || skipRedundantTars {
			tracelog.ErrorLogger.Fatalf("%s and %s settings are not supported yet",
				internal.UseReverseUnpackSetting, internal.SkipRedundantTarsSetting)
		}

		var extractProv postgres.ExtractProvider

		if onlyDatabases != nil {
			extractProv = greenplum.NewExtractProviderDBSpec(onlyDatabases)
		} else {
			extractProv = greenplum.ExtractProviderImpl{}
		}

		pgFetcher := postgres.GetPgFetcherOld(args[0], fileMask, restoreSpec, extractProv)
		internal.HandleBackupFetch(folder, targetBackupSelector, pgFetcher)
	},
}

// create the BackupSelector to select the segment backup to fetch
func createTargetFetchSegBackupSelector(cmd *cobra.Command,
	args []string, targetUserData string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) >= 2 {
		targetName = args[1]
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName, postgres.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	segBackupFetchCmd.Flags().StringVar(&fileMask, "mask", "", maskFlagDescription)
	segBackupFetchCmd.Flags().StringVar(&restoreSpec, "restore-spec", "", restoreSpecDescription)
	segBackupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data",
		"", targetUserDataDescription)
	segBackupFetchCmd.PersistentFlags().IntVar(&contentID, "content-id", 0, "segment content ID")
	_ = segBackupFetchCmd.MarkFlagRequired("content-id")
	segBackupFetchCmd.Flags().StringSliceVar(&onlyDatabases, "restore-only", nil, restoreOnlyDescription)
	// Since this is a utility command called by backup-fetch, it should not be exposed to the end user.
	segBackupFetchCmd.Hidden = true
	cmd.AddCommand(segBackupFetchCmd)
}
