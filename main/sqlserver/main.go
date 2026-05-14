package main

import (
	_ "github.com/microsoft/go-mssqldb"
	"github.com/wal-g/wal-g/cmd/sqlserver"
)

func main() {
	sqlserver.Execute()
}
