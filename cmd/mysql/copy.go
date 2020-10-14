package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	copyName                   = "copy"
	copyBackupName = "backup"
	copyAllName = "all"
	backupCopyShortDescription = "copy specific or all backups"

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
		Short: backupCopyShortDescription,
	}

	copyBackupCmd = &cobra.Command{
		Use:   copyBackupName,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			db.HandleCopyBackup(fromConfigFile, toConfigFile, backupName)
		},
	}
	copyAllCmd = &cobra.Command{
		Use:   copyAllName,
		Short: backupCopyShortDescription,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			db.HandleCopyAll(fromConfigFile, toConfigFile)
		},
	}
)


func init() {

	copyCmd.Flags().StringVarP(&backupName, backupNameFlag, backupNameShorthand, "", backupNameDescription)
	copyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)


	copyBackupCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)

	copyCmd.AddCommand(copyBackupCmd)
	copyCmd.AddCommand(copyAllCmd)

	for _, e := range []string{
		toConfigFile,
		fromConfigFile,
		backupName,
	} {
		err := copyBackupCmd.MarkFlagRequired(e)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to init copy cmd %v", err)
		}
	}

	for _, e := range []string{
		toConfigFile,
		fromConfigFile,
	} {
		err := copyBackupCmd.MarkFlagFilename(e)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to init copy cmd %v", err)
		}
	}

	Cmd.AddCommand(copyBackupCmd)
}

