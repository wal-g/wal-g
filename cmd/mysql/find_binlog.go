package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	findBinlogShortDescription = "Find the last uploaded binlog before specified GTID"
)

var (
	findGtid      = ""
	findBinlogCmd = &cobra.Command{
		Use:   "binlog-find",
		Short: findBinlogShortDescription,
		PreRun: func(cmd *cobra.Command, args []string) {
			internal.RequiredSettings[internal.MysqlDatasourceNameSetting] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			mysql.HandleBinlogFind(folder, findGtid)
		},
	}
)

func init() {
	cmd.AddCommand(findBinlogCmd)
	findBinlogCmd.Flags().StringVarP(&findGtid, "--gtid", "g", "", "GTID to find. Default is @@GTID_EXECUTED on current server")
}
