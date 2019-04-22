package main

import (
	"github.com/wal-g/wal-g/cmd/mysql"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/main"
)

var mysqlConfigKeys = []string{
	"WALG_MYSQL_DATASOURCE_NAME",
	"WALG_MYSQL_BINLOG_DST",
	"WALG_MYSQL_BINLOG_END_TS",
	"WALG_MYSQL_SSL_CA",
}

func main() {
	internal.UpdateAllowedConfig(mysqlConfigKeys)
	config.Configure()
	mysql.Execute()
}
