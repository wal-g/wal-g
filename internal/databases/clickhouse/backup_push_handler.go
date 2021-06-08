package clickhouse

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupPush(uploader *internal.Uploader, backupCmd *exec.Cmd) {
	backupFolderRootPath, _ := internal.GetSetting(internal.ClickHouseBackupPath)

	backupName := getBackupName()
	appendBackupNameToCommand(backupCmd, backupName)
	_, stderr, err := utility.StartCommandWithStdoutStderr(backupCmd)
	if err := backupCmd.Wait(); err != nil {
		tracelog.ErrorLogger.Printf("backup command output:\n%s", stderr.String())
		tracelog.ErrorLogger.Fatalf("failed to run backup: %v", err)
	}

	backupFolderPath := path.Join(backupFolderRootPath, backupName)
	filePaths, err := getBackupFilePaths(backupFolderPath)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("failed to get backup filepaths: %v", err)
	}

	for _, filePath := range filePaths {
		err = uploadBackupFile(uploader, backupFolderRootPath, filePath)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("failed to push file: %v", err)
		}
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

func getBackupFilePaths(backupFolder string) ([]string, error) {
	var filePaths []string
	err := filepath.WalkDir(backupFolder,
		func(path string, info os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				filePaths = append(filePaths, path)
			}
			return nil
	})
	return filePaths, err
}

func uploadBackupFile(uploader *internal.Uploader, basePath string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(file, "")

	rootFolder := uploader.UploadingFolder
	defer func() { uploader.UploadingFolder = rootFolder }()
	relativeFilePath, err := filepath.Rel(basePath, filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to get relative path: %s", filePath)
	}
	dst, filename := filepath.Split(relativeFilePath)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(dst)

	err = uploader.UploadFile(file)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	return nil
}
