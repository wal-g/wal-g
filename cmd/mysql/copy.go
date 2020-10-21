package mysql

import (
	"github.com/spf13/cobra"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	copyName             = "copy"
	copyBackupName       = "backup"
	copyAllName          = "all"
	copyShortDescription = "copy specific or all backups"

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
	backupName     string
	fromConfigFile string
	toConfigFile   string

	copyCmd = &cobra.Command{
		Use:   copyName,
		Short: copyShortDescription,
	}

	copyBackupCmd = &cobra.Command{
		Use:  copyBackupName,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			db.HandleCopyBackup(fromConfigFile, toConfigFile, backupName)
		},
	}
	copyAllCmd = &cobra.Command{
		Use:   copyAllName,
		Short: copyShortDescription,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			db.HandleCopyAll(fromConfigFile, toConfigFile)
		},
	}
)

func init() {

	copyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	copyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)

	Cmd.AddCommand(copyCmd)

	copyBackupCmd.Flags().StringVarP(&backupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)

	copyCmd.AddCommand(copyBackupCmd)
	copyCmd.AddCommand(copyAllCmd)
}
