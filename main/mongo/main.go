package main

import (
	"github.com/wal-g/wal-g/cmd/mongo"
	config "github.com/wal-g/wal-g/main"
)

func main() {
	config.Configure()
	mongo.Execute()
}
