package gp

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

const (
	backupMergeShortDescription = "Merges incremental backups into a full backup"
)

var (
	noCleanup bool

	backupMergeCmd = &cobra.Command{
		Use:   "backup-merge target_backup_name",
		Short: backupMergeShortDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			targetBackupName := args[0]

			logsDir := viper.GetString(conf.GPLogsDirectory)

			segPollInterval, err := conf.GetDurationSetting(conf.GPSegmentsPollInterval)
			tracelog.ErrorLogger.FatalOnError(err)

			segPollRetries := viper.GetInt(conf.GPSegmentsPollRetries)

			rootFolder, err := getMultistorageRootFolder(true, policies.TakeFirstStorage)
			tracelog.ErrorLogger.FatalOnError(err)

			uploader, err := internal.ConfigureUploaderToFolder(rootFolder)
			tracelog.ErrorLogger.FatalOnError(err)

			segmentFwdArgs := []greenplum.SegmentFwdArg{}
			doCleanup := !noCleanup // default to cleanup; user can disable with --no-cleanup
			arguments := greenplum.NewBackupMergeArguments(uploader, targetBackupName, segmentFwdArgs, logsDir,
				segPollInterval, segPollRetries, doCleanup)
			mergeHandler, err := greenplum.NewBackupMergeHandler(&arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			err = mergeHandler.HandleBackupMerge()
			tracelog.ErrorLogger.FatalOnError(err)
		},
	}
)

func init() {
	backupMergeCmd.Flags().BoolVar(&noCleanup, "no-cleanup", false, "Do not delete old incremental chain or garbage after merge")
	cmd.AddCommand(backupMergeCmd)
}
