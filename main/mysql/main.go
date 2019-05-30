package main

import (
	mysqlCmd "github.com/wal-g/wal-g/cmd/mysql"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/main"
)

var mysqlConfigKeys = []string{
	mysql.DatasourceNameSetting,
	mysql.BinlogDstSetting,
	mysql.BinlogSrcSetting,
	mysql.BinlogEndTsSetting,
	mysql.SslCaSetting,
}

func main() {
	internal.UpdateAllowedConfig(mysqlConfigKeys)
	config.Configure()
	mysqlCmd.Execute()
}
