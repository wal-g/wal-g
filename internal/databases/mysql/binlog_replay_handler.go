package mysql

import (
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const binlogFetchAhead = 2

type replayHandler struct {
	logCh           chan string
	errCh           chan error
	endTS           string
	backupBinlogPos BinlogPos

	// Test seams for the skip/replay decision logic; default to the real
	// implementations in newReplayHandler.
	getPreviousGTIDs func(filename, flavor string) (gomysql.GTIDSet, error)
	doReplay         func(ctx context.Context, binlogPath string) error
}

func newReplayHandler(ctx context.Context, endTS time.Time, backupBinlogPos BinlogPos) *replayHandler {
	rh := new(replayHandler)
	rh.endTS = endTS.Local().Format(TimeMysqlFormat)
	rh.backupBinlogPos = backupBinlogPos
	rh.getPreviousGTIDs = GetBinlogPreviousGTIDs
	rh.doReplay = rh.replayLog
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)
	go rh.replayLogs(ctx)
	return rh
}

// parseMariadbGTIDChecked parses gtidStr as a MariaDB GTID set, returning nil
// if it's empty or not MariaDB-format (MySQL GTID, or none recorded).
func parseMariadbGTIDChecked(gtidStr string) *gomysql.MariadbGTIDSet {
	if gtidStr == "" {
		return nil
	}
	parsed, err := gomysql.ParseMariadbGTIDSet(gtidStr)
	if err != nil {
		return nil
	}
	set, ok := parsed.(*gomysql.MariadbGTIDSet)
	if !ok {
		return nil
	}
	return set
}

func (rh *replayHandler) replayLogs(ctx context.Context) {
	backupBinlogNum := -1
	if rh.backupBinlogPos.FileName != "" {
		backupBinlogNum = BinlogNum(rh.backupBinlogPos.FileName)
	}

	// appliedGTID is the backup's GTID checkpoint (nil for MySQL, or
	// MariaDB <10.8). We skip binlogs it already covers -- needed after a
	// failover, when binlog file numbers no longer line up across servers.
	appliedGTID := parseMariadbGTIDChecked(rh.backupBinlogPos.LastGTID)

	// pendingPath is a binlog whose skip/replay fate isn't decided yet --
	// we need the next binlog's starting GTID checkpoint to know.
	var pendingPath string
	skippingByGTID := appliedGTID != nil
	var runErr error

loop:
	for binlogPath := range rh.logCh {
		binlogName := path.Base(binlogPath)

		if BinlogNum(binlogName) < backupBinlogNum {
			tracelog.InfoLogger.Printf("skipping %s (before backup boundary %s)", binlogName, rh.backupBinlogPos.FileName)
			os.Remove(binlogPath)
			continue
		}

		if pendingPath != "" {
			err := rh.resolvePending(ctx, appliedGTID, pendingPath, binlogPath, &skippingByGTID)
			pendingPath = ""
			if err != nil {
				runErr = err
				break loop
			}
		}

		if skippingByGTID {
			pendingPath = binlogPath
			continue
		}

		if err := rh.replayAndRemove(ctx, binlogPath); err != nil {
			runErr = err
			break loop
		}
	}

	if runErr == nil && pendingPath != "" {
		// nothing left to compare against -- replay it to be safe.
		runErr = rh.replayAndRemove(ctx, pendingPath)
	}

	if runErr != nil {
		rh.errCh <- runErr
	}
	close(rh.errCh)
}

// resolvePending checks whether pendingPath is fully covered by
// appliedGTID, using nextPath's GTID checkpoint. On any failure or new
// content it replays pendingPath and turns off GTID skipping for the rest
// of the run.
func (rh *replayHandler) resolvePending(
	ctx context.Context, appliedGTID *gomysql.MariadbGTIDSet, pendingPath, nextPath string, skippingByGTID *bool,
) error {
	pendingName := path.Base(pendingPath)

	endState, err := rh.getPreviousGTIDs(nextPath, gomysql.MariaDBFlavor)
	if err != nil {
		tracelog.WarningLogger.Printf(
			"could not determine GTID checkpoint for %s (from %s): %v -- replaying it to be safe",
			pendingName, path.Base(nextPath), err)
		*skippingByGTID = false
		return rh.replayAndRemove(ctx, pendingPath)
	}
	endGTID, ok := endState.(*gomysql.MariadbGTIDSet)
	if !ok || endGTID == nil {
		tracelog.WarningLogger.Printf("unexpected GTID set type for %s -- replaying it to be safe", pendingName)
		*skippingByGTID = false
		return rh.replayAndRemove(ctx, pendingPath)
	}

	if appliedGTID.Contain(endGTID) {
		tracelog.InfoLogger.Printf("skipping %s (already covered by backup GTID checkpoint %s)", pendingName, appliedGTID.String())
		os.Remove(pendingPath)
		return nil
	}

	tracelog.InfoLogger.Printf("replaying %s (introduces GTIDs beyond backup checkpoint %s)", pendingName, appliedGTID.String())
	*skippingByGTID = false
	return rh.replayAndRemove(ctx, pendingPath)
}

func (rh *replayHandler) replayAndRemove(ctx context.Context, binlogPath string) error {
	binlogName := path.Base(binlogPath)
	tracelog.InfoLogger.Printf("replaying %s ...", binlogName)
	err := rh.doReplay(ctx, binlogPath)
	os.Remove(binlogPath)
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to replay %s: %v", binlogName, err)
	}
	return err
}

func (rh *replayHandler) replayLog(ctx context.Context, binlogPath string) error {
	cmd, err := internal.GetCommandSettingContext(ctx, conf.MysqlBinlogReplayCmd)
	if err != nil {
		return err
	}
	env := os.Environ()
	env = append(env,
		fmt.Sprintf("%s=%s", "WALG_MYSQL_CURRENT_BINLOG", binlogPath),
		fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_END_TS", rh.endTS),
	)

	if rh.backupBinlogPos.LastGTID != "" {
		env = append(env, fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_LAST_GTID", rh.backupBinlogPos.LastGTID))
	}

	startPosition := rh.backupBinlogPos.FilePosition
	binlogName := path.Base(binlogPath)
	if binlogName == rh.backupBinlogPos.FileName && startPosition > 0 {
		tracelog.InfoLogger.Printf("replaying %s from position %d (backup boundary)", binlogName, startPosition)
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

func HandleBinlogReplay(ctx context.Context, folder storage.Folder, backupName string, untilTS string, untilBinlogLastModifiedTS string) {
	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	backup, err := internal.GetBackupByName(ctx, backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup: %v", err)

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(ctx, &sentinel)
	tracelog.ErrorLogger.FatalfOnError("Unable to fetch sentinel: %v", err)

	startTS, endTS, endBinlogTS, err := getTimestampsFromSentinel(ctx, folder, &sentinel, backup.Name, untilTS, untilBinlogLastModifiedTS)
	tracelog.ErrorLogger.FatalOnError(err)

	pos := sentinel.GetBinlogPos()
	if pos.FileName != "" {
		tracelog.InfoLogger.Printf("Backup binlog boundary: file=%s position=%d gtid=%s", pos.FileName, pos.FilePosition, pos.LastGTID)
	}

	handler := newReplayHandler(ctx, endTS, pos)

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

	var sentinel StreamSentinelDto
	err = backup.FetchSentinel(ctx, &sentinel)
	tracelog.ErrorLogger.FatalfOnError("Unable to fetch sentinel: %v", err)

	return getTimestampsFromSentinel(ctx, folder, &sentinel, backup.Name, untilTS, untilBinlogLastModifiedTS)
}

func getTimestampsFromSentinel(
	ctx context.Context, folder storage.Folder, sentinel *StreamSentinelDto,
	backupName, untilTS, untilBinlogLastModifiedTS string,
) (time.Time, time.Time, time.Time, error) {
	startTS, err := getBinlogSinceTS(ctx, folder, backupName, sentinel)
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
