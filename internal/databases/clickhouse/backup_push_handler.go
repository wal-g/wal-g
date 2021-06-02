package clickhouse

import (
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupPush(uploader internal.UploaderProvider, backupCmd *exec.Cmd) {
	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	_, err = uploader.PushStream(stdout)
	tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("backup create command failed: %v", err)
	}
}
