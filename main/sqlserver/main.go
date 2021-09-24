package main

import (
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/wal-g/wal-g/cmd/sqlserver"
)

func main() {
	sqlserver.Execute()
}
