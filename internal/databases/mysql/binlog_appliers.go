package mysql

import (
	"bufio"
	"bytes"
	"path"

	"github.com/tinsane/tracelog"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

type Applier = func(logFolder storage.Folder, logName string, settings BinlogFetchSettings) (needAbortFetch bool, err error)

var FSDownloadApplier = func(logFolder storage.Folder, logName string, settings BinlogFetchSettings) (needAbortFetch bool, err error) {
	dstPathFolder, err := settings.GetDestFolderPath()
	if err != nil {
		tracelog.ErrorLogger.Println(err)
		return true, err
	}

	pathToLog := path.Join(dstPathFolder, logName)
	tracelog.InfoLogger.Printf("downloading %s into %s", logName, pathToLog)
	if err = internal.DownloadWALFileTo(logFolder, logName, pathToLog); err != nil {
		tracelog.ErrorLogger.Print(err)
		return true, err
	}
	timestamp, err := parseFromBinlog(pathToLog)
	if err != nil {
		return true, err
	}
	return isBinlogCreatedAfterEndTs(*timestamp, settings.endTS), nil
}

var StreamApplier = func(logFolder storage.Folder, logName string, settings BinlogFetchSettings) (needAbortFetch bool, err error) {
	reader, err := internal.DownloadAndDecompressWALFile(logFolder, logName)
	buffReader := bufio.NewReaderSize(reader, 10*utility.Mebibyte)
	header, err := buffReader.Peek(TotalRequiredLen)
	if err != nil {
		return true, err
	}

	timestamp, err := parseFirstTimestampFromHeader(bytes.NewReader(header))
	if err != nil {
		return true, err
	}
	needAbort := isBinlogCreatedAfterEndTs(int32toTime(timestamp), settings.endTS)
	if needAbort {
		return true, nil
	}
	cmd, err := internal.GetCommandSetting(internal.MysqlBinlogApplyCmd)
	if err != nil {
		return true, err
	}
	cmd.Stdin = buffReader
	tracelog.InfoLogger.Printf("applying %s ...", logName)

	err = cmd.Run()
	if err != nil {
		tracelog.ErrorLogger.Printf("failed to apply %s: %v", logName, err)
		return true, err
	}
	return false, nil
}
