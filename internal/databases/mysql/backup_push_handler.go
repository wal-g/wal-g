package mysql

import (
	"os"
	"os/exec"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogStart := getMySQLCurrentBinlogFile(db)
	timeStart := utility.TimeNowCrossPlatformLocal()

	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	uncompressedSize := int64(0)
	streamReader := internal.NewWithSizeReader(limiters.NewDiskLimitReader(stdout), &uncompressedSize)
	fileName, err := uploader.PushStream(streamReader)
	tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)

	err = backupCmd.Wait()
	if err != nil {
		tracelog.ErrorLogger.Printf("Backup command output:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("backup create command failed: %v", err)
	}

	binlogEnd := getMySQLCurrentBinlogFile(db)
	timeStop := utility.TimeNowCrossPlatformLocal()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
		tracelog.WarningLogger.Printf("Failed to obtain the OS hostname for the backup sentinel\n")
	}
	sentinel := StreamSentinelDto{
		BinLogStart:      binlogStart,
		BinLogEnd:        binlogEnd,
		StartLocalTime:   timeStart,
		StopLocalTime:    timeStop,
		Hostname:         hostname,
		CompressedSize:   *uploader.TarSize,
		UncompressedSize: uncompressedSize,
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", sentinel.String())

	err = postgres.UploadSentinel(uploader, &sentinel, fileName)
	tracelog.ErrorLogger.FatalOnError(err)
}
