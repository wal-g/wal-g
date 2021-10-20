package mysql

import (
	"os"
	"os/exec"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd, isPermanent bool, userDataRaw string,
	partitions int, blockSize uint) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogStart := getMySQLCurrentBinlogFile(db)
	timeStart := utility.TimeNowCrossPlatformLocal()

	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	tracelog.ErrorLogger.FatalfOnError("failed to start backup create command: %v", err)

	var fileName string
	var backupType string
	if partitions == 0 || partitions == 1 {
		fileName, err = uploader.PushStream(limiters.NewDiskLimitReader(stdout))
		backupType = SplitMergeStreamBackup
		tracelog.ErrorLogger.FatalfOnError("failed to push backup: %v", err)
	} else {
		fileName, err = uploader.SplitAndPushStream(limiters.NewDiskLimitReader(stdout), partitions, int(blockSize))
		backupType = SingleStreamStreamBackup
		tracelog.ErrorLogger.FatalfOnError("failed to split and push backup: %v", err)
	}

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

	uploadedSize, err := uploader.UploadedDataSize()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to calc uploaded data size: %v", err)
	}
	// handle tiny backups:
	if partitions > 1 && uploadedSize < int64(blockSize)*int64(partitions) {
		partitions = int(uploadedSize / int64(blockSize))
		if uploadedSize%int64(blockSize) != 0 {
			partitions++
		}
	}

	rawSize, err := uploader.RawDataSize()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to calc raw data size: %v", err)
	}

	userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

	sentinel := StreamSentinelDto{
		BinLogStart:      binlogStart,
		BinLogEnd:        binlogEnd,
		StartLocalTime:   timeStart,
		StopLocalTime:    timeStop,
		Hostname:         hostname,
		CompressedSize:   uploadedSize,
		UncompressedSize: rawSize,
		IsPermanent:      isPermanent,
		UserData:         userData,
		Type:             backupType,
		Partitions:       partitions,
		BLockSize:        blockSize,
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", sentinel.String())

	err = internal.UploadSentinel(uploader, &sentinel, fileName)
	tracelog.ErrorLogger.FatalOnError(err)
}
