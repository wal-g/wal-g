package mysql

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
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

	// Test seams; default to the real implementations in newReplayHandler.
	getBinlogServerID   func(filename string) (uint32, error)
	getBinlogServerUUID func(filename string) (string, error)
	doReplay            func(ctx context.Context, binlogPath string) error
}

func newReplayHandler(ctx context.Context, endTS time.Time, backupBinlogPos BinlogPos) *replayHandler {
	rh := new(replayHandler)
	rh.endTS = endTS.Local().Format(TimeMysqlFormat)
	rh.backupBinlogPos = backupBinlogPos
	rh.getBinlogServerID = GetBinlogServerID
	rh.getBinlogServerUUID = GetBinlogServerUUID
	rh.doReplay = rh.replayLog
	rh.logCh = make(chan string, binlogFetchAhead)
	rh.errCh = make(chan error, 1)
	go rh.replayLogs(ctx)
	return rh
}

func (rh *replayHandler) replayLogs(ctx context.Context) {
	for binlogPath := range rh.logCh {
		binlogName := path.Base(binlogPath)

		tracelog.InfoLogger.Printf("replaying %s ...", binlogName)
		err := rh.doReplay(ctx, binlogPath)
		os.Remove(binlogPath)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to replay %s: %v", binlogName, err)
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
		fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_END_TS", rh.endTS),
	)

	if rh.backupBinlogPos.LastGTID != "" {
		env = append(env, fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_LAST_GTID", rh.backupBinlogPos.LastGTID))
	}

	binlogName := path.Base(binlogPath)
	// Safe even though this may not be the first binlog fetched: the
	// backup tool guarantees everything before startPosition in this
	// specific file is already covered by the backup.
	if rh.shouldApplyStartPosition(binlogPath) {
		startPosition := rh.backupBinlogPos.FilePosition
		tracelog.InfoLogger.Printf("replaying %s from position %d (backup boundary)", binlogName, startPosition)
		env = append(env,
			fmt.Sprintf("%s=%s", "WALG_MYSQL_BINLOG_START_POSITION", strconv.FormatInt(startPosition, 10)),
		)
	}
	cmd.Env = env
	return cmd.Run()
}

// shouldApplyStartPosition rejects a same-named file from a different
// server after a failover. Verification is flavor-specific: MariaDB GTIDs
// carry a numeric server ID directly; MySQL GTIDs identify the source by
// UUID instead, read from the candidate file's own first GTID_EVENT.
func (rh *replayHandler) shouldApplyStartPosition(binlogPath string) bool {
	binlogName := filepath.Base(binlogPath) // local file, not a storage path
	if binlogName != rh.backupBinlogPos.FileName || rh.backupBinlogPos.FilePosition <= 0 {
		return false
	}
	if rh.backupBinlogPos.LastGTID == "" {
		return true
	}

	_, flavor := parseGTIDChecked(rh.backupBinlogPos.LastGTID)
	switch flavor {
	case gomysql.MariaDBFlavor:
		backupGTID, err := gomysql.ParseMariadbGTID(rh.backupBinlogPos.LastGTID)
		if err != nil {
			return false
		}
		actualServerID, err := rh.getBinlogServerID(binlogPath)
		if err != nil {
			tracelog.WarningLogger.Printf(
				"could not verify server id of %s: %v -- not trusting the backup boundary position",
				binlogName, err)
			return false
		}
		return actualServerID == backupGTID.ServerID
	case gomysql.MySQLFlavor:
		backupUUID, _, ok := strings.Cut(rh.backupBinlogPos.LastGTID, ":")
		if !ok {
			return false
		}
		actualUUID, err := rh.getBinlogServerUUID(binlogPath)
		if err != nil {
			tracelog.WarningLogger.Printf(
				"could not verify server uuid of %s: %v -- not trusting the backup boundary position",
				binlogName, err)
			return false
		}
		return strings.EqualFold(actualUUID, backupUUID)
	default:
		// Recorded but not parseable in a known flavor -- can't verify
		// either way, so don't trust a coincidental filename match.
		return false
	}
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

	// checkpoint is nil for MySQL, or MariaDB <10.8 -- the filter then just
	// forwards everything, same as before GTID-skip existed.
	checkpoint, flavor := parseGTIDChecked(pos.LastGTID)
	filter := newGTIDSkipFilter(handler, checkpoint, flavor)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTS, endTS)
	err = fetchLogs(ctx, folder, dstDir, startTS, endTS, endBinlogTS, filter)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = filter.flush()
	tracelog.ErrorLogger.FatalfOnError("Failed to apply binlogs: %v", err)

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
