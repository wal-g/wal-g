package gp

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/postgres/orioledb"
	"github.com/wal-g/wal-g/internal/walparser"
)

const (
	backupCopyUsage            = "copy"
	backupCopyShortDescription = "copy specific or all backups"
	backupCopyLongDescription  = "Copy Greenplum backup(s) without transforming payloads"

	backupNameFlag        = "backup-name"
	backupNameShorthand   = "b"
	backupNameDescription = "Copy specific backup"

	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy backup"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy backup"

	withHistoryFlag        = "with-history"
	withHistoryShorthand   = "w"
	withHistoryDescription = "Synchronize every segment WAL stream through the latest cluster restore point"

	walWaitTimeoutFlag        = "wal-wait-timeout"
	walWaitTimeoutDescription = "How long to wait for restore point WAL and metadata the source has not published yet, 0 to fail at once"
)

var (
	copyOutput       string
	copyOutputFile   string
	targetBackupName string
	fromConfigFile   string
	toConfigFile     string
	withHistory      bool
	walWaitTimeout   time.Duration

	backupCopyCmd = &cobra.Command{
		Use:   backupCopyUsage,
		Short: backupCopyShortDescription,
		Long:  backupCopyLongDescription,
		Args:  cobra.ExactArgs(0),
		Run:   runBackupCopy,
		PersistentPreRun: func(*cobra.Command, []string) {
			if viper.IsSet(conf.PgWalSize) {
				postgres.SetWalSize(viper.GetUint64(conf.PgWalSize))
			}
			if viper.IsSet(conf.PgWalPageSize) {
				walparser.SetWalPageSize(viper.GetUint64(conf.PgWalPageSize))
			}
			if viper.IsSet(conf.PgBlockSize) {
				walparser.SetBlockSize(viper.GetUint64(conf.PgBlockSize))
				postgres.SetDatabasePageSize(viper.GetUint64(conf.PgBlockSize))
				orioledb.SetDatabasePageSize(viper.GetUint64(conf.PgBlockSize))
			}
		},
	}
)

func runBackupCopy(cmd *cobra.Command, args []string) {
	if cmd.Flags().Changed("output") {
		if copyOutput != "rclone" {
			tracelog.ErrorLogger.Fatalf("unsupported --output %q: expected rclone", copyOutput)
		}
		if copyOutputFile == "" {
			tracelog.ErrorLogger.Fatalf("--output-file must not be empty")
		}
		from, _, err := internal.StorageAndConfigFromFile(cmd.Context(), fromConfigFile)
		tracelog.ErrorLogger.FatalOnError(err)
		plan, err := greenplum.BuildCopyPlanWithWALWait(
			cmd.Context(), from.RootFolder(), nil, targetBackupName, withHistory, walWaitTimeout)
		tracelog.ErrorLogger.FatalOnError(err)
		if err := plan.WriteRcloneFileList(copyOutputFile); err != nil {
			tracelog.ErrorLogger.Fatalf("write rclone file list %q: %v", copyOutputFile, err)
		}
		tracelog.InfoLogger.Printf("Wrote rclone file list to %q.", copyOutputFile)
		return
	}
	greenplum.HandleCopyWithHistory(cmd.Context(), fromConfigFile, toConfigFile, targetBackupName, withHistory, walWaitTimeout)
}

func init() {
	cmd.AddCommand(backupCopyCmd)

	backupCopyCmd.Flags().StringVarP(&targetBackupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)
	backupCopyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	backupCopyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	backupCopyCmd.Flags().BoolVarP(&withHistory, withHistoryFlag, withHistoryShorthand, false, withHistoryDescription)
	backupCopyCmd.Flags().DurationVar(&walWaitTimeout, walWaitTimeoutFlag, greenplum.DefaultCopyWALWaitTimeout,
		walWaitTimeoutDescription)

	backupCopyCmd.Flags().StringVar(&copyOutput, "output", "", "write a file list instead of copying: rclone")
	backupCopyCmd.Flags().StringVar(&copyOutputFile, "output-file", "", "path to the output file list")
	backupCopyCmd.MarkFlagsRequiredTogether("output", "output-file")
	backupCopyCmd.MarkFlagsOneRequired("to", "output")
	_ = backupCopyCmd.MarkFlagRequired(fromFlag)
}
