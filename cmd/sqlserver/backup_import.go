package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupImportShortDescription = "Import backups from the external storage"

var externalConfigFileImport string
var importDatabases = make(map[string]string)

var backupImportCmd = &cobra.Command{
	Use:   "backup-import",
	Short: backupImportShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleBackupImport(externalConfigFileImport, importDatabases)
	},
}

func init() {
	backupImportCmd.Flags().StringVarP(&externalConfigFileImport, "external-config", "e", "", "wal-g config file for external storage")
	backupImportCmd.Flags().StringToStringVarP(&importDatabases, "databases", "d", nil,
		"list of databases to import, mapped to the list of .bak files in the external storage, "+
			"eg. -d db1=db1_1.bak,db1_2.bak -d db2=old.bak")
	cmd.AddCommand(backupImportCmd)
}
