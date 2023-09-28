package mysql

import (
	"github.com/wal-g/tracelog"
	"os"
	"os/exec"
	"strings"
)

type XtrabackupInfo struct {
}

func isXtrabackup(cmd *exec.Cmd) bool {
	for _, arg := range cmd.Args {
		if strings.Index(arg, "xtrabackup") != -1 || strings.Index(arg, "xbstream") != -1 {
			return true
		}
	}
	return false
}

func prepareXtrabackupExtraDirectory(backupCmd *exec.Cmd) (string, error) {
	tmpDirRoot := "/tmp"
	tmpDirPattern := "wal-g"
	tmpPath, err := os.MkdirTemp(tmpDirRoot, tmpDirPattern)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to create temporary directory in %s/%s", tmpDirRoot, tmpDirPattern)
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
	return XtrabackupInfo{}, nil
}

func enrichBackupArgs(backupCmd *exec.Cmd, xtrabackupExtraDirectory string, isFullBackup bool, prevBackupInfo PrevBackupInfo) error {
	// –extra-lsndir=DIRECTORY - save an extra copy of the xtrabackup_checkpoints and xtrabackup_info files in this directory.
	backupCmd.Args = append(backupCmd.Args, "–extra-lsndir="+xtrabackupExtraDirectory)

	if !isFullBackup && (prevBackupInfo != PrevBackupInfo{}) {
		// –incremental-lsn=LSN
		var lsn = prevBackupInfo.sentinel.LSN.String()
		backupCmd.Args = append(backupCmd.Args, "–incremental-lsn="+lsn)
	}

	return nil
}
