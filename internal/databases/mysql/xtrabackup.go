package mysql

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/databases/mysql/xbstream"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
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

type XtrabackupExtInfo struct {
	XtrabackupInfo
	ServerOS   string
	ServerArch string
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
func prepareTemporaryDirectory(tmpDirRoot string) (string, error) {
	tmpDirPattern := "wal-g"
	tmpPath, err := os.MkdirTemp(tmpDirRoot, tmpDirPattern)

	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to create temporary directory like %s/%s", tmpDirRoot, tmpDirPattern)
		tracelog.ErrorLogger.Fatalf("Failed to create temporary directory: %v", err)
	}

	return tmpPath, nil
}

func removeTemporaryDirectory(tmpDir string) error {
	err := os.RemoveAll(tmpDir)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to remove temporary directory in %s", tmpDir)
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

func GetXtrabackupFetcher(restoreCmd, prepareCmd *exec.Cmd, useXbtoolExtract bool) func(folder storage.Folder, backup internal.Backup) {
	return func(folder storage.Folder, backup internal.Backup) {
		err := xtrabackupFetch(backup.Name, folder, restoreCmd, prepareCmd, useXbtoolExtract, true)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)
	}
}

func xtrabackupFetch(
	backupName string,
	folder storage.Folder,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd,
	useXbtoolExtract bool,
	isLast bool) error {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	// common procedure: start from base backup & apply diffs one by one
	// recursively, find base backup and start from it:
	if sentinel.IsIncremental {
		// check required configs earlier:
		_, err = internal.GetCommandSetting(conf.MysqlBackupPrepareCmd)
		tracelog.ErrorLogger.FatalfOnError("%v", err)

		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *sentinel.IncrementFrom, *sentinel.IncrementFromLSN)
		err = xtrabackupFetch(*sentinel.IncrementFrom, folder, restoreCmd, prepareCmd, useXbtoolExtract, false)
		if err != nil {
			return err
		}
	}

	if useXbtoolExtract {
		return xtrabackupFetchInhouse(backup, prepareCmd, isLast)
	}
	return xtrabackupFetchClassic(backup, restoreCmd, prepareCmd, isLast)
}

func xtrabackupFetchClassic(backup internal.Backup, restoreCmd *exec.Cmd, prepareCmd *exec.Cmd, isLast bool) error {
	// Manually we will do the following:
	//
	// xbstream -x -C /var/lib/mysql
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql
	//
	// xbstream -x -C /data/inc1 < INC1.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql --incremental-dir=/data/inc1
	//
	// xbstream -x -C /data/inc2 < INC2.xbstream
	// xtrabackup --prepare                  --target-dir=/var/lib/mysql --incremental-dir=/data/inc2

	var sentinel StreamSentinelDto
	err := backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	incrementalBackupDir := viper.GetString(conf.MysqlIncrementalBackupDst)
	tempDeltaDir, err := prepareTemporaryDirectory(incrementalBackupDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to prepare temp dir: %v", err)

	if sentinel.IsIncremental {
		restoreCmd = cloneCommand(restoreCmd)
		restoreArgs := strings.Fields(restoreCmd.Args[len(restoreCmd.Args)-1])
		replaceCommandArgument(restoreCmd, restoreArgs[len(restoreArgs)-1], tempDeltaDir)

		prepareCmd = cloneCommand(prepareCmd)
		injectCommandArgument(prepareCmd, XtrabackupIncrementalDir+"="+tempDeltaDir)
	}
	if !isLast {
		prepareCmd = cloneCommand(prepareCmd)
		injectCommandArgument(prepareCmd, XtrabackupApplyLogOnly)
	}

	stdin, err := restoreCmd.StdinPipe()
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)
	stderr := &bytes.Buffer{}
	restoreCmd.Stderr = stderr

	tracelog.InfoLogger.Printf("Restoring %s with cmd %v", backup.Name, restoreCmd.Args)
	err = restoreCmd.Start()
	if err != nil {
		return err
	}
	fetcher, err := internal.GetBackupStreamFetcher(backup)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to detect backup format: %v\n", err)
		return err
	}
	err = fetcher(backup, stdin)
	cmdErr := restoreCmd.Wait()
	if cmdErr != nil {
		tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
		err = cmdErr
	}
	if err != nil {
		return err
	}
	tracelog.InfoLogger.Printf("Restored %s", backup.Name)

	if prepareCmd != nil {
		tracelog.InfoLogger.Printf("Preparing %s with cmd %v", backup.Name, prepareCmd.Args)
		prepareCmd.Stdout = os.Stdout
		prepareCmd.Stderr = os.Stderr
		err = prepareCmd.Run()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to prepare fetched backup: %v", err)
			return err
		}
		tracelog.InfoLogger.Printf("Prepared %s", backup.Name)
	} else {
		tracelog.InfoLogger.Printf("WALG_MYSQL_BACKUP_PREPARE_COMMAND not configured. Skipping prepare phase")
	}

	return os.RemoveAll(tempDeltaDir)
}

func xtrabackupFetchInhouse(backup internal.Backup, prepareCmd *exec.Cmd, isLast bool) error {
	// This is equivalent to:
	//
	// wal-g xb extract --decompress /var/lib/mysql  < BASE.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql
	//
	// wal-g xb extract --decompress /data/inc1  < INC1.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql --incremental-dir=/data/inc1
	//
	// wal-g xb extract --decompress /data/inc2  < INC2.xbstream
	// xtrabackup --prepare                  --target-dir=/var/lib/mysql --incremental-dir=/data/inc2

	var sentinel StreamSentinelDto
	err := backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	dataDir, err := internal.GetLogsDstSettings(conf.MysqlDataDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to get config value: %v", err)

	incrementalBackupDir := viper.GetString(conf.MysqlIncrementalBackupDst)
	tempDeltaDir, err := prepareTemporaryDirectory(incrementalBackupDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to prepare temp dir: %v", err)

	if sentinel.IsIncremental {
		prepareCmd = cloneCommand(prepareCmd)
		injectCommandArgument(prepareCmd, XtrabackupIncrementalDir+"="+tempDeltaDir)
	}
	if !isLast {
		prepareCmd = cloneCommand(prepareCmd)
		injectCommandArgument(prepareCmd, XtrabackupApplyLogOnly)
	}

	fetcher, err := internal.GetBackupStreamFetcher(backup)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to detect backup format: %v\n", err)
		return err
	}

	destinationDir := tempDeltaDir
	if !sentinel.IsIncremental {
		destinationDir = dataDir
	}
	var wg sync.WaitGroup
	reader, writer := io.Pipe()
	streamReader := xbstream.NewReader(reader, false)
	wg.Add(1)
	go xbstream.AsyncDiskSink(&wg, streamReader, destinationDir, true)

	err = fetcher(backup, writer)
	if err != nil {
		tracelog.ErrorLogger.Printf("Restore failed: %v", err)
		return err
	}
	wg.Wait()
	tracelog.InfoLogger.Printf("Restored %s", backup.Name)

	if prepareCmd != nil {
		tracelog.InfoLogger.Printf("Preparing %s with cmd %v", backup.Name, prepareCmd.Args)
		prepareCmd.Stdout = os.Stdout
		prepareCmd.Stderr = os.Stderr
		err = prepareCmd.Run()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to prepare fetched backup: %v", err)
			return err
		}
		tracelog.InfoLogger.Printf("Prepared %s", backup.Name)
	} else {
		tracelog.InfoLogger.Printf("WALG_MYSQL_BACKUP_PREPARE_COMMAND not configured. Skipping prepare phase")
	}

	return os.RemoveAll(tempDeltaDir)
}
