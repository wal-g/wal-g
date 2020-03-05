package mysql

import (
	"bufio"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"time"
)

func replayHandler(logFolder storage.Folder, logName string, endTs time.Time) (needAbortFetch bool, err error) {
	reader, err := internal.DownloadAndDecompressWALFile(logFolder, logName)
	binlogReader := NewBinlogReader(reader, time.Unix(0, 0), endTs)
	bufReader := bufio.NewReaderSize(binlogReader, 10*utility.Mebibyte)
	cmd, err := internal.GetCommandSetting(internal.MysqlBinlogReplayCmd)
	if err != nil {
		return true, err
	}
	cmd.Stdin = bufReader
	tracelog.InfoLogger.Printf("replaying %s ...", logName)
	err = cmd.Run()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to replay %s: %v", logName, err)
		return true, err
	}
	return binlogReader.NeedAbort(), nil
}

func HandleBinlogReplay(folder storage.Folder, backupName string, untilDT string) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %v", err)

	startTs, err := getBinlogStartTs(folder, backup)
	tracelog.ErrorLogger.FatalOnError(err)

	endTs, err := configureEndTs(untilDT)
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	_, err = fetchLogs(folder, startTs, endTs, replayHandler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch logs: %v", err)
}
