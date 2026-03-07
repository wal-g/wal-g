package mysql

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const binlogFetchAhead = 2

type backupBinlogInfo struct {
	fileName     string
	filePosition int64
}

type replayHandler struct {
	logCh        chan string
	errCh        chan error
	endTS        string
	backupBinlog backupBinlogInfo
	appliedFirst bool
}

func newReplayHandler(endTS time.Time, info backupBinlogInfo) *replayHandler {
	rh := new(replayHandler)
	rh.endTS = endTS.Local().Format(TimeMysqlFormat)
	rh.backupBinlog = info
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)
	go rh.replayLogs()
	return rh
}

func (rh *replayHandler) replayLogs() {
	for binlogPath := range rh.logCh {
		binlogName := path.Base(binlogPath)

		if !rh.appliedFirst && rh.backupBinlog.fileName != "" {
			if binlogName < rh.backupBinlog.fileName {
				tracelog.InfoLogger.Printf("skipping %s (before backup boundary %s)", binlogName, rh.backupBinlog.fileName)
				os.Remove(binlogPath)
				continue
			}

			rh.appliedFirst = true

			if binlogName == rh.backupBinlog.fileName && rh.backupBinlog.filePosition > 0 {
				tracelog.InfoLogger.Printf("replaying %s from position %d (backup boundary)", binlogName, rh.backupBinlog.filePosition)
				err := rh.replayLog(binlogPath, rh.backupBinlog.filePosition)
				os.Remove(binlogPath)
				if err != nil {
					tracelog.ErrorLogger.Printf("failed to replay %s: %v", binlogName, err)
					rh.errCh <- err
					break
				}
				continue
			}
		}

		tracelog.InfoLogger.Printf("replaying %s ...", binlogName)
		err := rh.replayLog(binlogPath, 0)
		os.Remove(binlogPath)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to replay %s: %v", binlogName, err)
			rh.errCh <- err
			break
		}
	}
	close(rh.errCh)
}

func (rh *replayHandler) replayLog(binlogPath string, startPosition int64) error {
	cmd, err := internal.GetCommandSetting(conf.MysqlBinlogReplayCmd)
	if err != nil {
		return err
	}
	env := os.Environ()
	env = append(env,
		fmt.Sprintf("%s=%s", "WALG_MYSQL_CURRENT_BINLOG", binlogPath),
		fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_END_TS", rh.endTS),
	)
	if startPosition > 0 {
		env = append(env,
			fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_START_POSITION", strconv.FormatInt(startPosition, 10)),
		)
	}
	cmd.Env = env
	return cmd.Run()
}

func (rh *replayHandler) wait() error {
	close(rh.logCh)
	return <-rh.errCh
}

func (rh *replayHandler) handleBinlog(binlogPath string) error {
	select {
	case err := <-rh.errCh:
		return err
	case rh.logCh <- binlogPath:
		return nil
	}
}

func HandleBinlogReplay(folder storage.Folder, backupName string, untilTS string, untilBinlogLastModifiedTS string) {
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	startTS, endTS, endBinlogTS, err := getTimestamps(folder, backupName, untilTS, untilBinlogLastModifiedTS)
	tracelog.ErrorLogger.FatalOnError(err)

	info, err := getBackupBinlogInfo(folder, backupName)
	if err != nil {
		tracelog.WarningLogger.Printf("Could not determine backup binlog info: %v", err)
	}
	if info.fileName != "" {
		tracelog.InfoLogger.Printf("Backup binlog boundary: file=%s position=%d", info.fileName, info.filePosition)
	}

	handler := newReplayHandler(endTS, info)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTS, endTS)
	err = fetchLogs(folder, dstDir, startTS, endTS, endBinlogTS, handler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = handler.wait()
	tracelog.ErrorLogger.FatalfOnError("Failed to apply binlogs: %v", err)
}

func getTimestamps(folder storage.Folder, backupName, untilTS, untilBinlogLastModifiedTS string) (time.Time, time.Time, time.Time, error) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, errors.Wrap(err, "Unable to get backup")
	}

	startTS, err := getBinlogSinceTS(folder, backup)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	endTS, err := utility.ParseUntilTS(untilTS)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	endBinlogTS, err := utility.ParseUntilTS(untilBinlogLastModifiedTS)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}
	return startTS, endTS, endBinlogTS, nil
}

func getBackupBinlogInfo(folder storage.Folder, backupName string) (backupBinlogInfo, error) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return backupBinlogInfo{}, errors.Wrap(err, "Unable to get backup")
	}
	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		return backupBinlogInfo{}, errors.Wrap(err, "Unable to fetch sentinel")
	}
	return backupBinlogInfo{
		fileName:     sentinel.BinLogFileName,
		filePosition: sentinel.BinLogFilePosition,
	}, nil
}
