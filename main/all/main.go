package main

import (
	all_cmd "github.com/wal-g/wal-g/cmd/all"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/main"
)

func main() {
	internal.UpdateAllowedConfig(mysql.AllowedConfigKeys)
	config.Configure()
	all_cmd.Execute()
}
