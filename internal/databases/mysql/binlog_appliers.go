package mysql

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type Applier = func(logDst string) error

var NoopApplier = func(logDst string) error {
	return nil
}

func GetCommandApplier(untilDt string) Applier {
	return func(logDst string) error {
		cmd := internal.GetLogApplyCmd()
		tracelog.InfoLogger.Printf("Use apply command (%v)", cmd)
		tracelog.InfoLogger.Printf("Applying on (%v)", logDst)
		if err := internal.ApplyCommand(cmd, []string{untilDt, logDst})(); err != nil {
			tracelog.ErrorLogger.Print(err)
			return err
		}
		return nil
	}
}