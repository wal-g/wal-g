package main

import (
	"github.com/wal-g/wal-g/cmd/pg"
	"github.com/wal-g/wal-g/main"
)

func main() {
	config.Configure()
	pg.Execute()
}
