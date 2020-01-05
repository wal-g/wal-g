package internal

import (
	"io"
	"io/ioutil"
	"os/exec"

	"github.com/wal-g/tracelog"
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

func ApplyCommand(command []string, stdin io.Reader) error {
	cmd := exec.Command(command[0], command[1:]...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		tracelog.ErrorLogger.Printf("cmd.Run() failed with %s\n", err)
		return err
	}
	tracelog.InfoLogger.Printf("combined out:\n%s\n", string(out))
	return nil
}
