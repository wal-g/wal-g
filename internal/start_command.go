package internal

import (
	"github.com/tinsane/tracelog"
	"io"
	"os/exec"
)

func StartCommand(command []string) (waitFunc func() error, stdout io.ReadCloser) {
	c := exec.Command(command[0], command[1:]...)
	stdoutResult, err := c.StdoutPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	err = c.Start()
	waitFuncResult := c.Wait
	tracelog.ErrorLogger.FatalOnError(err)
	return waitFuncResult, stdoutResult
}