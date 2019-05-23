package main

import (
	"github.com/wal-g/wal-g/cmd/mysql"
	interconfig "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/main"
)

var mysqlConfigKeys = []string{
	"WALG_MYSQL_DATASOURCE_NAME",
	"WALG_MYSQL_BINLOG_DST",
	"WALG_MYSQL_BINLOG_SRC",
	"WALG_MYSQL_BINLOG_END_TS",
	"WALG_MYSQL_SSL_CA",
}

func main() {
	interconfig.UpdateAllowed(mysqlConfigKeys)
	config.Configure()
	mysql.Execute()
}
