package internal

import (
	"bytes"
	"io"
	"os/exec"

	"github.com/wal-g/tracelog"
)

func StartCommand(command []string) (waitFunc func(), stdout io.ReadCloser) {
	c := exec.Command(command[0], command[1:]...)
	stdoutResult, err := c.StdoutPipe()
	tracelog.ErrorLogger.FatalOnError(err)
	stderr := &bytes.Buffer{}
	c.Stderr = stderr
	err = c.Start()
	waitFuncResult := c.Wait
	tracelog.ErrorLogger.FatalOnError(err)
	return func() {
		err = waitFuncResult()
		if err != nil {
			tracelog.ErrorLogger.Printf("%s stderr:\n%s\n", command[0], stderr.String())
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
