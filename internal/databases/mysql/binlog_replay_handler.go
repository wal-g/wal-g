package mysql

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const binlogFetchAhead = 2

type replayHandler struct {
	logCh chan string
	errCh chan error
	endTs string
}

func newReplayHandler(endTs time.Time) *replayHandler {
	rh := new(replayHandler)
	rh.endTs = endTs.Local().Format(TimeMysqlFormat)
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)
	go rh.replayLogs()
	return rh
}

func (rh *replayHandler) replayLogs() {
	for binlogPath := range rh.logCh {
		tracelog.InfoLogger.Printf("replaying %s ...", path.Base(binlogPath))
		err := rh.replayLog(binlogPath)
		os.Remove(binlogPath)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to replay %s: %v", path.Base(binlogPath), err)
			rh.errCh <- err
			break
		}
	}
	close(rh.errCh)
}

func (rh *replayHandler) replayLog(binlogPath string) error {
	cmd, err := internal.GetCommandSetting(internal.MysqlBinlogReplayCmd)
	if err != nil {
		return err
	}
	env := os.Environ()
	env = append(env, fmt.Sprintf("%s=%s", "WALG_MYSQL_CURRENT_BINLOG", binlogPath))
	env = append(env, fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_END_TS", rh.endTs))
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

func HandleBinlogReplay(folder storage.Folder, backupName string, untilTs string) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %v", err)

	startTs, err := getBinlogSinceTs(folder, backup)
	tracelog.ErrorLogger.FatalOnError(err)

	endTs, err := utility.ParseUntilTs(untilTs)
	tracelog.ErrorLogger.FatalOnError(err)

	dstDir, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	handler := newReplayHandler(endTs)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	err = fetchLogs(folder, dstDir, startTs, endTs, handler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = handler.wait()
	tracelog.ErrorLogger.FatalfOnError("Failed to apply binlogs: %v", err)
}
