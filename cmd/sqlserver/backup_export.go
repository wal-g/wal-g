package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const backupExportShortDescription = "Export backups to the external storage"

var externalConfigFileExport string
var exportDatabases = make(map[string]string)

var backupExportCmd = &cobra.Command{
	Use:   "backup-export",
	Short: backupExportShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleBackupExport(externalConfigFileExport, exportDatabases)
	},
}

func init() {
	backupExportCmd.Flags().StringVarP(&externalConfigFileExport, "external-config", "e", "", "wal-g config file for external storage")
	backupExportCmd.Flags().StringToStringVarP(&exportDatabases, "databases", "d", nil,
		"list of databases to export, mapped to the prefixes of .bak files in the external storage, "+
			"eg. -d db1=db1 -d db2=db2copy")
	cmd.AddCommand(backupExportCmd)
}
