package mysql

import (
	"github.com/wal-g/tracelog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
			break
		case "to_lsn":
			result.ToLSN = ParseLSN(value)
			break
		case "last_lsn":
			result.LastLSN = ParseLSN(value)
			break
		}
	}
	return result
}

func isXtrabackup(cmd *exec.Cmd) bool {
	for _, arg := range cmd.Args {
		if strings.Index(arg, "xtrabackup") != -1 || strings.Index(arg, "xbstream") != -1 {
			return true
		}
	}
	return false
}

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

func removeXtrabackupExtraDirectory(xtrabackupExtraDirectory string) error {
	err := os.RemoveAll(xtrabackupExtraDirectory)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to remove temporary directory in %s", xtrabackupExtraDirectory)
		return nil // don't crash the app
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

func enrichBackupArgs(backupCmd *exec.Cmd, xtrabackupExtraDirectory string, isFullBackup bool, prevBackupInfo PrevBackupInfo) error {
	// -–extra-lsndir=DIRECTORY - save an extra copy of the xtrabackup_checkpoints and xtrabackup_info files in this directory.
	injectArg(backupCmd, "--extra-lsndir", xtrabackupExtraDirectory)

	if !isFullBackup && (prevBackupInfo != PrevBackupInfo{} && prevBackupInfo.sentinel.LSN != nil) {
		// –-incremental-lsn=LSN
		injectArg(backupCmd, "--incremental-lsn", prevBackupInfo.sentinel.LSN.String())
	}

	return nil
}

func injectArg(cmd *exec.Cmd, key string, value string) {
	// NA: It is unintuitive, but internal.GetCommandSetting() calls `/bin/sh -c <command>`
	//     and when we are adding new arg to cmd.Args array - it won't be passed to xtrabackup
	//     so, add it to the last argument (that we expect to be our backup tool arg).
	cmd.Args[len(cmd.Args)-1] += " " + key + "=" + value
}
