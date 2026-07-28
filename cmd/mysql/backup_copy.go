package mysql

import (
	"fmt"

	"github.com/spf13/cobra"
	db "github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	copyName             = "backup-copy"
	copyShortDescription = "Copy specific or all backups"

	copyAllFlag         = "all"
	copyAllSDescription = "copy all backups"
	allShorthand        = "a"

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

	copyCmd = &cobra.Command{
		Use:   copyName,
		Short: copyShortDescription,
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if all == (backupName != "") {
				return fmt.Errorf("exactly one of --all or --backup must be specified")
			}
			if all {
				if prefix != "" {
					return fmt.Errorf("--add-prefix cannot be used with --all")
				}
				db.HandleCopyAll(cmd.Context(), fromConfigFile, toConfigFile)
				return nil
			}
			db.HandleCopyBackup(cmd.Context(), fromConfigFile, toConfigFile, backupName, prefix)
			return nil
		},
		PersistentPreRun: func(*cobra.Command, []string) {},
	}
)

func init() {
	copyCmd.Flags().StringVarP(&toConfigFile, toFlag, toShorthand, "", toDescription)
	copyCmd.Flags().StringVarP(&fromConfigFile, fromFlag, fromShorthand, "", fromDescription)
	copyCmd.Flags().StringVarP(&backupName, backupNameFlag, backupShorthand, "", backupShortDescription)
	copyCmd.Flags().StringVarP(&prefix, prefixFlag, prefixShorthand, "", prefixDescription)
	copyCmd.Flags().BoolVarP(&all, copyAllFlag, allShorthand, false, copyAllSDescription)
	_ = copyCmd.MarkFlagRequired(fromFlag)
	_ = copyCmd.MarkFlagRequired(toFlag)

	cmd.AddCommand(copyCmd)
}
