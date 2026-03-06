package mysql

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/wal-g/wal-g/internal/databases/mysql/xbstream"

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
	// Binlog file and position at the time of backup (parsed from xtrabackup_info/mariadb_backup_info)
	BinLogFileName     string
	BinLogFilePosition int64
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
		case "binlog_pos":
			result.BinLogFileName, result.BinLogFilePosition = parseBinlogPos(value)
		}
	}
	return result
}

// parseBinlogPos extracts binlog filename and position from the binlog_pos field
// in xtrabackup_info / mariadb_backup_info.
// Formats:
//
//	"filename 'mysql-bin.000002', position '607', GTID of the last change '0-1-7'" (mariabackup)
//	"filename 'mysql-bin.000003', position '154'" (xtrabackup)
func parseBinlogPos(value string) (string, int64) {
	var fileName string
	var filePos int64

	if idx := strings.Index(value, "filename '"); idx >= 0 {
		rest := value[idx+len("filename '"):]
		if end := strings.Index(rest, "'"); end >= 0 {
			fileName = rest[:end]
		}
	}

	if idx := strings.Index(value, "position '"); idx >= 0 {
		rest := value[idx+len("position '"):]
		if end := strings.Index(rest, "'"); end >= 0 {
			if pos, err := strconv.ParseInt(rest[:end], 10, 64); err == nil {
				filePos = pos
			}
		}
	}

	return fileName, filePos
}

func isXtrabackup(cmd *exec.Cmd) bool {
	for _, arg := range cmd.Args {
		if strings.Contains(arg, "xtrabackup") || strings.Contains(arg, "xbstream") || strings.Contains(arg, "mariabackup") {
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

// readFileWithFallback tries primary path, falling back to alternate.
// Supports both xtrabackup_* (MySQL/older MariaDB) and mariadb_backup_* (MariaDB 11.8+) naming.
func readFileWithFallback(dir, primary, fallback string) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(dir, primary))
	if err == nil {
		return raw, nil
	}
	return os.ReadFile(filepath.Join(dir, fallback))
}

func readXtrabackupInfo(xtrabackupExtraDirectory string) (XtrabackupInfo, error) {
	checkpointsRaw, err := readFileWithFallback(
		xtrabackupExtraDirectory,
		"xtrabackup_checkpoints",
		"mariadb_backup_checkpoints",
	)
	if err != nil {
		return XtrabackupInfo{}, err
	}
	result := NewXtrabackupInfo(string(checkpointsRaw))

	infoRaw, err := readFileWithFallback(
		xtrabackupExtraDirectory,
		"xtrabackup_info",
		"mariadb_backup_info",
	)
	if err != nil {
		tracelog.WarningLogger.Printf("Could not read backup info file for binlog position: %v", err)
		return result, nil
	}
	infoResult := NewXtrabackupInfo(string(infoRaw))
	result.BinLogFileName = infoResult.BinLogFileName
	result.BinLogFilePosition = infoResult.BinLogFilePosition

	return result, nil
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

func GetXtrabackupFetcher(restoreCmd, prepareCmd *exec.Cmd, useXbtoolExtract bool, inplace bool) internal.Fetcher {
	return func(folder storage.Folder, backup internal.Backup) {
		err := xtrabackupFetch(backup.Name, folder, restoreCmd, prepareCmd, useXbtoolExtract, inplace, true)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v", err)
	}
}

func xtrabackupFetch(
	backupName string,
	folder storage.Folder,
	restoreCmd *exec.Cmd,
	prepareCmd *exec.Cmd,
	useXbtoolExtract bool,
	inplace bool,
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
		err = xtrabackupFetch(*sentinel.IncrementFrom, folder, restoreCmd, prepareCmd, useXbtoolExtract, inplace, false)
		if err != nil {
			return err
		}
	}

	if useXbtoolExtract {
		return xtrabackupFetchInhouse(backup, prepareCmd, inplace, isLast)
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

func xtrabackupFetchInhouse(backup internal.Backup, prepareCmd *exec.Cmd, inplace bool, isLast bool) error {
	// This is equivalent to:
	//
	// wal-g xb [extract|extract-diff] --decompress /var/lib/mysql  < BASE.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql
	//
	// wal-g xb [extract|extract-diff] --decompress [--incremental-dir] /data/inc1   < INC1.xbstream
	// xtrabackup --prepare --apply-log-only --target-dir=/var/lib/mysql --incremental-dir=/data/inc1
	//
	// wal-g xb [extract|extract-diff] --decompress [--incremental-dir] /data/inc2  < INC2.xbstream
	// xtrabackup --prepare                  --target-dir=/var/lib/mysql --incremental-dir=/data/inc2

	var sentinel StreamSentinelDto
	err := backup.FetchSentinel(&sentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch sentinel: %v", err)

	dataDir, err := internal.GetLogsDstSettings(conf.MysqlDataDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to get config value: %v", err)

	incrementalBackupDir, err := internal.GetLogsDstSettings(conf.MysqlIncrementalBackupDst)
	tracelog.ErrorLogger.FatalfOnError("Failed to get config value: %v", err)
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

	var wg sync.WaitGroup
	reader, writer := io.Pipe()
	streamReader := xbstream.NewReader(reader, false)
	wg.Add(1)

	if inplace && sentinel.IsIncremental {
		// apply diff-files to dataDir inplace (and leave required leftovers incrementalDir)
		// nolint : staticcheck
		go xbstream.AsyncDiffBackupSink(&wg, streamReader, dataDir, tempDeltaDir)
	} else {
		destinationDir := tempDeltaDir
		if !sentinel.IsIncremental {
			destinationDir = dataDir
		}
		go xbstream.AsyncBackupSink(&wg, streamReader, destinationDir, true)
	}

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
