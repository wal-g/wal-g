package mysql

import (
	"context"
	"os/exec"
	"strings"
)

func injectCommandArgument(cmd *exec.Cmd, argument string) {
	// NA: It is unintuitive, but internal.GetCommandSetting() calls `/bin/sh -c <command>`
	//     and when we are adding new arg to cmd.Args array - it won't be passed to xtrabackup
	//     so, add it to the last argument (that we expect to be our backup tool arg).
	cmd.Args[len(cmd.Args)-1] += " " + argument
}

func replaceCommandArgument(cmd *exec.Cmd, replace, argument string) {
	// NA: It is unintuitive, but internal.GetCommandSetting() calls `/bin/sh -c <command>`
	//     and whole <command> will be last argument. So, patch last argument (that we expect to be our backup tool arg).
	cmd.Args[len(cmd.Args)-1] = strings.Replace(cmd.Args[len(cmd.Args)-1], replace, argument, 1)
}

func cloneCommand(cmd *exec.Cmd) *exec.Cmd {
	return exec.CommandContext(context.Background(), cmd.Args[0], cmd.Args[1:]...)
}
