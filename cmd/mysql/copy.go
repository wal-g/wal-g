package mysql

import (
	"github.com/spf13/cobra"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	copyName             = "copy"
	copyShortDescription = "copy specific or all backups"

	copyAllFlag             = "all"
	copyAllSDescription = "copy all backups"
	allShorthand   = "a"

	backupNameFlag         = "backup"
	backupShorthand        = "b"
	backupShortDescription = "copy target backup"

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
	all bool

	copyCmd = &cobra.Command{
		Use:   copyName,
		Short: copyShortDescription,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if all {
				db.HandleCopyAll(fromConfigFile, toConfigFile)
				return
			}
			db.HandleCopyBackup(fromConfigFile, toConfigFile, backupName)
		},
	}
)

func init() {

	copyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	copyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	copyCmd.Flags().StringVarP(&backupName, backupNameFlag, backupShorthand, "", backupShortDescription)
	copyCmd.Flags().BoolVarP(&all, copyAllFlag, allShorthand, false, copyAllSDescription)

	Cmd.AddCommand(copyCmd)
}
