package internal

import (
	"github.com/tinsane/tracelog"
	"io"
	"io/ioutil"
	"os/exec"
)

func StartCommand(command []string) (waitFunc func(), stdout io.ReadCloser) {
	c := exec.Command(command[0], command[1:]...)
	stdoutResult, err := c.StdoutPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	stderrResult, err := c.StderrPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	err = c.Start()
	waitFuncResult := c.Wait
	tracelog.ErrorLogger.FatalOnError(err)
	return func() {
		var err error
		var errorString string
		if errorBytes, err := ioutil.ReadAll(stderrResult); err == nil {
			errorString = string(errorBytes)
		}
		tracelog.ErrorLogger.FatalOnError(err)

		err = waitFuncResult()
		if err != nil {
			tracelog.ErrorLogger.Println(errorString)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}, stdoutResult
}

func RestoreCommand(command []string) (waitFunc func(), stdout io.WriteCloser) {
	c := exec.Command(command[0], command[1:]...)
	stdinWriteCLoser, err := c.StdinPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	stderrResult, err := c.StderrPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	err = c.Start()
	waitFuncResult := c.Wait
	tracelog.ErrorLogger.FatalOnError(err)
	return func() {
		var err error
		var errorString string
		if errorBytes, err := ioutil.ReadAll(stderrResult); err == nil {
			errorString = string(errorBytes)
		}
		tracelog.ErrorLogger.FatalOnError(err)

		err = waitFuncResult()
		if err != nil {
			tracelog.ErrorLogger.Println(errorString)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}, stdinWriteCLoser
}
