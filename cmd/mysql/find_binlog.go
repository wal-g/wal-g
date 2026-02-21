package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/logging"
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
			conf.RequiredSettings[conf.MysqlDatasourceNameSetting] = true
			err := internal.AssertRequiredSettingsSet()
			logging.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			storage, err := internal.ConfigureStorage()
			logging.FatalOnError(err)
			mysql.HandleBinlogFind(storage.RootFolder(), findGtid)
		},
	}
)

func init() {
	cmd.AddCommand(findBinlogCmd)
	findBinlogCmd.Flags().StringVarP(&findGtid, "--gtid", "g", "", "GTID to find. Default is @@GTID_EXECUTED on current server")
}
