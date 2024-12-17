package gp

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	backupCopyUsage            = "copy"
	backupCopyShortDescription = "copy specific backup"
	backupCopyLongDescription  = "Copy backup from one storage to another according to configs " +
		"(with history by default)"

	backupNameFlag        = "backup-name"
	backupNameShorthand   = "b"
	backupNameDescription = "Copy specific backup"

	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy backup"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy backup"
)

var (
	targetBackupName string
	fromConfigFile   string
	toConfigFile     string

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
		},
	}
)

func runBackupCopy(cmd *cobra.Command, args []string) {
	greenplum.HandleCopy(fromConfigFile, toConfigFile, targetBackupName)
}

func init() {
	cmd.AddCommand(backupCopyCmd)

	backupCopyCmd.Flags().StringVarP(&targetBackupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)
	backupCopyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	backupCopyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)

	_ = backupCopyCmd.MarkFlagRequired(backupNameFlag)
	_ = backupCopyCmd.MarkFlagRequired(toFlag)
	_ = backupCopyCmd.MarkFlagRequired(fromFlag)
}
