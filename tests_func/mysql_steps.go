package functests

import (
	"fmt"
	"github.com/cucumber/godog"
	"github.com/wal-g/wal-g/tests_func/helpers"
)

func (tctx *TestContext) testMySQLConnect(host string) error {
	return godog.ErrPending
}

func (tctx *TestContext) followingBashScriptFinishedWithResult(script, host string, expectedResult int) error {
	container := tctx.ContainerFQDN(host)

	// run bash script
	result, err := helpers.RunCommandStrict(tctx.Context, container, []string{"/bin/bash", script})
	if err != nil {
		return err
	}
	if result.ExitCode != expectedResult {
		return fmt.Errorf("exit code (%v) does not match expected result (%v)", result.ExitCode, expectedResult)
	}
	return nil
}

func SetupMySQLSteps(ctx *godog.ScenarioContext, tctx *TestContext) {
	ctx.Step(`^a working mysql on ([^\s]*)$`, tctx.testMySQLConnect)
	ctx.Step(`^bash script \"([^\s]*)\" executed on host \"([^\s]*)\" finished with result \'(\d+)\'$`, tctx.followingBashScriptFinishedWithResult)
}
