package mysql

import (
	"bytes"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	XtrabackupApplyLogOnly   = "--apply-log-only"
	XtrabackupIncrementalDir = "--incremental-dir"
)

type XtrabackupInfo struct {
	FromLSN *LSN
	// LSN that xtrabackup observed when backup started
	ToLSN *LSN
	// max LSN that were observed at the end of the backup
	LastLSN *LSN
}

func NewXtrabackupInfo(content string) XtrabackupInfo {
	result := XtrabackupInfo{}
	for _, line := range strings.Split(content, "\n") {
		pair := strings.SplitN(line, "=", 2)
		if len(pair) != 2 {
			continue
		}
		key := strings.TrimSpace(pair[0])
		value := strings.TrimSpace(pair[1])
		switch key {
		case "from_lsn":
			result.FromLSN = ParseLSN(value)
		case "to_lsn":
			result.ToLSN = ParseLSN(value)
		case "last_lsn":
			result.LastLSN = ParseLSN(value)
		}
	}
	return result
}

func isXtrabackup(cmd *exec.Cmd) bool {
	for _, arg := range cmd.Args {
		if strings.Contains(arg, "xtrabackup") || strings.Contains(arg, "xbstream") {
			return true
		}
	}
	return false
}

//nolint:unparam
func prepareXtrabackupExtraDirectory() (string, error) {
	tmpDirRoot := "/tmp" // There is no Percona XtraBackup for Windows (c) @PeterZaitsev
	tmpDirPattern := "wal-g"
	tmpPath, err := os.MkdirTemp(tmpDirRoot, tmpDirPattern)

	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to create temporary directory like %s/%s", tmpDirRoot, tmpDirPattern)
		tracelog.ErrorLogger.Fatalf("Failed to create temporary directory: %v", err)
	}

	return tmpPath, nil
}

//nolint:unparam
func removeXtrabackupExtraDirectory(xtrabackupExtraDirectory string) error {
	err := os.RemoveAll(xtrabackupExtraDirectory)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to remove temporary directory in %s", xtrabackupExtraDirectory)
		return err
	}
	return nil
}

func readXtrabackupInfo(xtrabackupExtraDirectory string) (XtrabackupInfo, error) {
	raw, err := os.ReadFile(filepath.Join(xtrabackupExtraDirectory, "xtrabackup_checkpoints"))
	if err != nil {
		return XtrabackupInfo{}, err
	}
	return NewXtrabackupInfo(string(raw)), nil
}

func enrichBackupArgs(backupCmd *exec.Cmd, xtrabackupExtraDirectory string, isFullBackup bool, prevBackupInfo *PrevBackupInfo) {
	if prevBackupInfo == nil {
		tracelog.ErrorLogger.Fatalf("PrevBackupInfo is null")
	}
	// -–extra-lsndir=DIRECTORY - save an extra copy of the xtrabackup_checkpoints and xtrabackup_info files in this directory.
	injectCommandArgument(backupCmd, "--extra-lsndir="+xtrabackupExtraDirectory)

	if !isFullBackup && (*prevBackupInfo != PrevBackupInfo{} && prevBackupInfo.sentinel.LSN != nil) {
		// –-incremental-lsn=LSN
		injectCommandArgument(backupCmd, "--incremental-lsn="+prevBackupInfo.sentinel.LSN.String())
	}
}

func GetXtrabackupFetcher(restoreCmd, prepareCmd *exec.Cmd) func(folder storage.Folder, backup internal.Backup) {
	return func(folder storage.Folder, backup internal.Backup) {
		err := xtrabackupFetch(backup.Name, folder, restoreCmd, prepareCmd, true)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)
	}
}

func xtrabackupFetch(
	backupName string,
	folder storage.Folder,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd,
	isLast bool) error {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	tempDeltaDir, err := prepareXtrabackupExtraDirectory()
	tracelog.ErrorLogger.FatalfOnError("Failed to prepare temp dir: %v", err)

	// common procedure:
	// xbstream -x -C /var/lib/mysql
	// xbstream -x -C /data/inc1 < INC1.xbstream
	// xbstream -x -C /data/inc2 < INC2.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql --incremental-dir=/data/inc1
	// xtrabackup --prepare                  --target-dir=/var/lib/mysql --incremental-dir=/data/inc2

	if sentinel.IsIncremental {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *sentinel.IncrementFrom, *sentinel.IncrementFromLSN)
		err = xtrabackupFetch(*sentinel.IncrementFrom, folder, restoreCmd, prepareCmd, false)
		if err != nil {
			return err
		}
	}

	if sentinel.IsIncremental {
		restoreCmd = cloneCommand(restoreCmd)
		restoreArgs := strings.Split(restoreCmd.Args[len(restoreCmd.Args)-1], " ")
		replaceCommandArgument(restoreCmd, restoreArgs[len(restoreArgs)-1], tempDeltaDir)

		prepareCmd = cloneCommand(prepareCmd)
		injectCommandArgument(prepareCmd, XtrabackupIncrementalDir+"="+tempDeltaDir)
	}
	if !isLast {
		injectCommandArgument(prepareCmd, XtrabackupApplyLogOnly)
	}

	stdin, err := restoreCmd.StdinPipe()
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)
	stderr := &bytes.Buffer{}
	restoreCmd.Stderr = stderr

	tracelog.InfoLogger.Printf("Restoring %s with cmd %v", backupName, restoreCmd.Args)
	err = restoreCmd.Start()
	if err != nil {
		return err
	}
	err = internal.DownloadAndDecompressStream(backup, stdin)
	if err != nil {
		return err
	}
	cmdErr := restoreCmd.Wait()
	if cmdErr != nil {
		tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
		err = cmdErr
	}
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Restored %s", backupName)

	if prepareCmd != nil {
		tracelog.InfoLogger.Printf("Preparing %s with cmd %v", backupName, prepareCmd.Args)
		err = prepareCmd.Run()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to prepare fetched backup: %v", err)
			return err
		}
		tracelog.InfoLogger.Printf("Prepared %s", backupName)
	}

	return os.RemoveAll(tempDeltaDir)
}
