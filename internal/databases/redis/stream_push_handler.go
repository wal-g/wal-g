package redis

import (
	"os/exec"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd) {
	// Configure folder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	backupName := "dump_" + time.Now().Format(time.RFC3339)
	compressed := internal.CompressAndEncrypt(stdout, uploader.Compressor, internal.ConfigureCrypter())
	err = uploader.Upload(backupName, compressed)
	tracelog.ErrorLogger.FatalOnError(err)

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("backup create command failed: %v", err)
	}
}
