package main

import (
	redisCmd "github.com/wal-g/wal-g/cmd/redis"
	"github.com/wal-g/wal-g/main"
)

func main() {
	config.Configure()
	redisCmd.Execute()
}
