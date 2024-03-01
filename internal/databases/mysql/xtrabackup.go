package mysql

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/cenkalti/backoff"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
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

func enrichBackupArgs(backupCmd *exec.Cmd, xtrabackupExtraDirectory string, isFullBackup bool, fifoStreams int, prevBackupInfo *PrevBackupInfo) {
	if prevBackupInfo == nil {
		tracelog.ErrorLogger.Fatalf("PrevBackupInfo is null")
	}
	// -–extra-lsndir=DIRECTORY - save an extra copy of the xtrabackup_checkpoints and xtrabackup_info files in this directory.
	injectCommandArgument(backupCmd, "--extra-lsndir="+xtrabackupExtraDirectory)

	if !isFullBackup && (*prevBackupInfo != PrevBackupInfo{} && prevBackupInfo.sentinel.LSN != nil) {
		// –-incremental-lsn=LSN
		injectCommandArgument(backupCmd, "--incremental-lsn="+prevBackupInfo.sentinel.LSN.String())
	}

	if fifoStreams > 1 {
		injectCommandArgument(backupCmd, fmt.Sprintf("--fifo-streams=%v", fifoStreams))
		// when --fifo-stream specified, --fifo-dir must be configured as well
		injectCommandArgument(backupCmd, "--fifo-dir=/tmp") // FIXME: make configurable
		injectCommandArgument(backupCmd, "--fifo-timeout=60")
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

	incrementalBackupDir := viper.GetString(conf.MysqlIncrementalBackupDst)
	tempDeltaDir, err := prepareTemporaryDirectory(incrementalBackupDir)
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
		restoreArgs := strings.Fields(restoreCmd.Args[len(restoreCmd.Args)-1])
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
	fetcher, err := GetBackupStreamFetcher(backup)
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

func GetBackupStreamFetcher(backup internal.Backup) (internal.StreamFetcher, error) {
	// There are 2 backup layouts for xtrabackup:
	// single-stream:
	//   /basebackups_005
	//  	/stream_20240228T115512Z
	//			<stream content>
	// multi-stream backups:
	//   /basebackups_005
	//  	/stream_20240228T115512Z
	// 			/thread_N   (where N in range 0..fifoStreams)
	// 				<stream content>

	_, folders, err := backup.Folder.ListFolder()
	if err != nil {
		return nil, fmt.Errorf("cannot list files in backup folder '%s' due to: %w", backup.Folder.GetPath(), err)
	}

	fifoStreams := 0
	for _, folder := range folders {
		if ok, _ := regexp.MatchString(".*/thread_\\d/", folder.GetPath()); ok {
			fifoStreams++
		}
	}

	if fifoStreams > 0 {
		return FifoMultiStreamFetcher(fifoStreams), nil
	} else {
		return internal.GetBackupStreamFetcher(backup)
	}
}

func FifoMultiStreamFetcher(fifoStreams int) internal.StreamFetcher {
	return func(backup internal.Backup, writeCloser io.WriteCloser) error {
		defer utility.LoggedClose(writeCloser, "")

		errGroup := new(errgroup.Group)
		for i := 0; i < fifoStreams; i++ {
			subBackup, err := internal.NewBackup(backup.Folder.GetSubFolder(fmt.Sprintf("thread_%v", i)), backup.Name)
			if err != nil {
				return err
			}
			fetcher, err := internal.GetBackupStreamFetcher(subBackup)
			if err != nil {
				return err
			}

			fifoFileName := getXtrabackupFifoFileName(i) // FIXME: new file dir?
			err = syscall.Mkfifo(fifoFileName, 0640)
			if err != nil {
				return err
			}
			// FIXME: delete fifos

			pipe, err := os.OpenFile(fifoFileName, os.O_WRONLY, os.ModeNamedPipe)
			if err != nil {
				return err
			}

			errGroup.Go(func() error {
				return fetcher(subBackup, pipe)
			})
		}

		return errGroup.Wait()
	}
}

func getXtrabackupFifoFileName(idx int) string {
	// fifo name format is hardcoded in xtrabackup source code:
	// https://github.com/percona/percona-xtrabackup/blob/percona-xtrabackup-8.0.35-30/storage/innobase/xtrabackup/src/xbstream.cc#L728
	return fmt.Sprintf("/tmp/thread_%v", idx) // FIXME: make configurable
}

func openFifoFile(fifoFileName string) (*os.File, error) {
	tracelog.DebugLogger.Printf("Openning %s", fifoFileName)
	var file *os.File
	err := retry(10, func() (err error) {
		file, err = os.OpenFile(fifoFileName, os.O_RDONLY, os.ModeNamedPipe)
		if err != nil {
			tracelog.DebugLogger.Printf("failed to open named pipe: %v", err)
		}
		return err
	})
	return file, err
}

func retry(maxRetries uint64, op func() error) error {
	var b backoff.BackOff
	b = backoff.NewExponentialBackOff()
	b = backoff.WithMaxRetries(b, maxRetries)
	return backoff.Retry(op, b)
}
