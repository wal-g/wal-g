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
	cleanup bool

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
			doCleanup := cleanup
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
	backupMergeCmd.Flags().BoolVar(&cleanup, "cleanup", false, "Delete old incremental chain and garbage after merge")
	cmd.AddCommand(backupMergeCmd)
}
