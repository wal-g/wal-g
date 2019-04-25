package main

import (
	pg_cmd "github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/main"
)

func main() {
	config.Configure()
	pg_cmd.Execute()
}
