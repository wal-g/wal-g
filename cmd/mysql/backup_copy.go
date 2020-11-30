package mysql

import (
	"github.com/spf13/cobra"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	copyName             = "backup-copy"
	copyShortDescription = "copy specific or all backups"

	copyAllFlag         = "all"
	copyAllSDescription = "copy all backups"
	allShorthand        = "a"

	overwriteFlag         = "force-overwrite"
	overwriteSDescription = "force overwrite when copying files"
	overwriteShorthand        = "w"

	backupNameFlag         = "backup"
	backupShorthand        = "b"
	backupShortDescription = "copy target backup"

	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy backup"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy backup"

	prefixFlag        = "add-prefix"
	prefixShorthand   = "p"
	prefixDescription = "add prefix to path"
)

var (
	backupName     string
	prefix         string
	fromConfigFile string
	toConfigFile   string
	all            bool
	forceOverwrite bool

	copyCmd = &cobra.Command{
		Use:   copyName,
		Short: copyShortDescription,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if all {
				db.HandleCopyAll(fromConfigFile, toConfigFile, forceOverwrite)
				return
			}
			db.HandleCopyBackup(fromConfigFile, toConfigFile, backupName, prefix, forceOverwrite)
		},
	}
)

func init() {

	copyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	copyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)

	copyCmd.Flags().StringVarP(&backupName, backupNameFlag, backupShorthand, "", backupShortDescription)

	copyCmd.Flags().StringVarP(&prefix, prefixFlag, prefixShorthand, "", prefixDescription)

	copyCmd.Flags().BoolVarP(&all, copyAllFlag, allShorthand, false, copyAllSDescription)
	copyCmd.Flags().BoolVarP(&forceOverwrite, overwriteFlag, overwriteShorthand, false, overwriteSDescription)

	Cmd.AddCommand(copyCmd)
}
