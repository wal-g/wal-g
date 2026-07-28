package mysql

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const binlogFetchAhead = 2

type replayHandler struct {
	logCh chan string
	errCh chan error
	endTS string
}

func newReplayHandler(ctx context.Context, endTS time.Time) *replayHandler {
	rh := new(replayHandler)
	rh.endTS = endTS.Local().Format(TimeMysqlFormat)
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)
	go rh.replayLogs(ctx)
	return rh
}

func (rh *replayHandler) replayLogs(ctx context.Context) {
	for binlogPath := range rh.logCh {
		tracelog.InfoLogger.Printf("replaying %s ...", path.Base(binlogPath))
		err := rh.replayLog(ctx, binlogPath)
		os.Remove(binlogPath)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to replay %s: %v", path.Base(binlogPath), err)
			rh.errCh <- err
			break
		}
	}
	close(rh.errCh)
}

func (rh *replayHandler) replayLog(ctx context.Context, binlogPath string) error {
	cmd, err := internal.GetCommandSettingContext(ctx, conf.MysqlBinlogReplayCmd)
	if err != nil {
		return err
	}
	env := os.Environ()
	env = append(env,
		fmt.Sprintf("%s=%s", "WALG_MYSQL_CURRENT_BINLOG", binlogPath),
		fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_END_TS", rh.endTS))
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

func HandleBinlogReplay(ctx context.Context, folder storage.Folder, backupName string, untilTS string, untilBinlogLastModifiedTS string) {
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	startTS, endTS, endBinlogTS, err := getTimestamps(ctx, folder, backupName, untilTS, untilBinlogLastModifiedTS)
	tracelog.ErrorLogger.FatalOnError(err)

	handler := newReplayHandler(ctx, endTS)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTS, endTS)
	err = fetchLogs(ctx, folder, dstDir, startTS, endTS, endBinlogTS, handler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = handler.wait()
	tracelog.ErrorLogger.FatalfOnError("Failed to apply binlogs: %v", err)
}

func getTimestamps(ctx context.Context,
	folder storage.Folder, backupName, untilTS, untilBinlogLastModifiedTS string) (time.Time, time.Time, time.Time, error) {
	backup, err := internal.GetBackupByName(ctx, backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, errors.Wrap(err, "Unable to get backup")
	}

	startTS, err := getBinlogSinceTS(ctx, folder, backup)
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
