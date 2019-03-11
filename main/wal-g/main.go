package main

import (
	"github.com/wal-g/wal-g/cmd"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

var pathToExtensions string

func main() {
	err := internal.LoadExtensions(pathToExtensions)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	err = internal.ConfigureLogging()
	if err != nil {
		tracelog.ErrorLogger.Println("Failed to configure logging.")
		tracelog.ErrorLogger.FatalError(err)
	}

	err = internal.ConfigureLimiters()
	if err != nil {
		tracelog.ErrorLogger.Println("Failed to configure limiters")
		tracelog.ErrorLogger.FatalError(err)
	}

	internal.RegisterExtensionCommands(cmd.RootCmd)

	cmd.Execute()
}
