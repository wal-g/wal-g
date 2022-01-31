package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

var binlogToolboxCmd = &cobra.Command{
	Use:   "binlog",
	Short: "binlog-related toolbox",
}

var forgetBinlogGtidsCmd = &cobra.Command{
	Use:   "forget-gtids",
	Short: "remove GTIDs from binlog sentinel",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploaderWithoutCompressMethod()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogForgetGtids(uploader)
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		internal.RequiredSettings[internal.MysqlDatasourceNameSetting] = true
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(binlogToolboxCmd)
	binlogToolboxCmd.AddCommand(forgetBinlogGtidsCmd)
}
