package gp

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
)

var (
	targetBackupName string
	fromConfigFile   string
	toConfigFile     string
	withHistory      bool

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
	greenplum.HandleCopyWithHistory(cmd.Context(), fromConfigFile, toConfigFile, targetBackupName, withHistory)
}

func init() {
	cmd.AddCommand(backupCopyCmd)

	backupCopyCmd.Flags().StringVarP(&targetBackupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)
	backupCopyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	backupCopyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	backupCopyCmd.Flags().BoolVarP(&withHistory, withHistoryFlag, withHistoryShorthand, false, withHistoryDescription)

	_ = backupCopyCmd.MarkFlagRequired(toFlag)
	_ = backupCopyCmd.MarkFlagRequired(fromFlag)
}
