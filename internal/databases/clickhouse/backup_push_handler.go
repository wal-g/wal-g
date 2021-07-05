package clickhouse

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/viper"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd, isPermanent bool) {
	backupFolderRootPath, ok := internal.GetSetting(internal.ClickHouseBackupPath)
	if !ok {
		backupFolderRootPath = "/var/lib/clickhouse/backup"
		tracelog.InfoLogger.Printf("%s is not set. Looking for backup in %s", internal.ClickHouseBackupPath, backupFolderRootPath)
	}

	timeStart := utility.TimeNowCrossPlatformLocal()

	backupName := getBackupName()
	appendBackupNameToCommand(backupCmd, backupName)
	stdout, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed starting backup command: %v", err)
	}
	backupOutput, err := ioutil.ReadAll(stdout)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed reading from backup stdout: %v", err)
	}
	if err := backupCmd.Wait(); err != nil {
		tracelog.ErrorLogger.Printf("backup command output:\n%s", backupOutput)
		tracelog.ErrorLogger.Printf("backup command stderr:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("failed to run backup: %v", err)
	}

	uploadBackup(uploader, backupName, path.Join(backupFolderRootPath, backupName))

	err = uploadSentinel(uploader, timeStart, backupName, isPermanent)
	if err != nil {
		tracelog.ErrorLogger.FatalOnError(err)
	}
}

func getBackupName() string {
	now := time.Now()
	localName := fmt.Sprintf(
		"%d-%02d-%02dT%02d-%02d-%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	return localName
}

func appendBackupNameToCommand(backupCmd *exec.Cmd, backupName string) {
	lastIndex := len(backupCmd.Args) - 1
	backupCmd.Args[lastIndex] = fmt.Sprintf("%s %s", backupCmd.Args[lastIndex], backupName)
}

func uploadBackup(uploader *internal.Uploader, backupName string, backupDirectory string) TarFileSets {
	bundle := NewBundle(backupDirectory, internal.ConfigureCrypter(), viper.GetInt64(internal.TarSizeThresholdSetting))

	// Start a new tar bundle, walk the backupDirectory and upload everything there.
	tracelog.InfoLogger.Println("Starting a new tar bundle")
	err := bundle.StartQueue(internal.NewStorageTarBallMaker(backupName, uploader))
	tracelog.ErrorLogger.FatalOnError(err)

	tarBallComposerMaker := NewTarBallComposerMaker()
	tracelog.ErrorLogger.FatalOnError(err)

	err = bundle.SetupComposer(tarBallComposerMaker)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(backupDirectory, bundle.HandleWalkedFSObject)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Println("Packing ...")
	tarFileSets, err := bundle.PackTarballs()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.DebugLogger.Println("Finishing queue ...")
	err = bundle.FinishQueue()
	tracelog.ErrorLogger.FatalOnError(err)

	uploader.Finish()

	return tarFileSets
}

func uploadSentinel(uploader internal.UploaderProvider, timeStart time.Time, backupName string, isPermanent bool) error {
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

	rawSize, err := uploader.RawDataSize()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to calc raw data size: %v", err)
	}

	sentinel := BackupSentinelDto{
		StartLocalTime:   timeStart,
		StopLocalTime:    timeStop,
		Hostname:         hostname,
		CompressedSize:   uploadedSize,
		UncompressedSize: rawSize,
		IsPermanent:      isPermanent,
	}

	tracelog.InfoLogger.Printf("Backup sentinel: %s", sentinel.String())

	return internal.UploadSentinel(uploader, &sentinel, backupName)
}
