package mysql

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type BinlogFetchSettings struct {
	startTs   time.Time
	endTS     *time.Time
	needApply bool
}

func (settings BinlogFetchSettings) GetLogsFetchInterval() (time.Time, *time.Time) {
	return settings.startTs, settings.endTS
}

func (settings BinlogFetchSettings) GetDestFolderPath() (string, error) {
	return internal.GetLogsDstSettings(BinlogDstSetting)
}

func (settings BinlogFetchSettings) GetLogFolderPath() string {
	return BinlogPath
}

type BinlogFetchHandler struct {
	settings     BinlogFetchSettings
	applier      Applier
	afterFetch   func([]storage.Object) error
	abortHandler func(string) error
}

func (handler BinlogFetchHandler) AfterFetch(logs []storage.Object) error {
	return handler.afterFetch(logs)
}

func (handler BinlogFetchHandler) HandleAbortFetch(logFileName string) error {
	tracelog.InfoLogger.Printf("handling abort fetch over %s", logFileName)
	return handler.abortHandler(logFileName)
}

func (handler BinlogFetchHandler) FetchLog(logFolder storage.Folder, logName string) (bool, error) {
	tracelog.InfoLogger.Printf("fetching log file %s", logName)
	return handler.applier(logFolder, logName, handler.settings)
}

var indexFileCreator = func(logsFolderPath string, logs []storage.Object) error {
	tracelog.InfoLogger.Printf("creating index file %s", logsFolderPath)
	return createIndexFile(logsFolderPath, logs)
}

func NewBinlogFetchHandler(settings BinlogFetchSettings) BinlogFetchHandler {
	if settings.needApply {
		return BinlogFetchHandler{
			settings: settings,
			applier:  StreamApplier,
			afterFetch: func(objects []storage.Object) error {
				return nil
			},
			abortHandler: func(s string) error {
				return nil
			},
		}
	}
	return BinlogFetchHandler{
		settings: settings,
		applier:  FSDownloadApplier,
		afterFetch: func(objects []storage.Object) error {
			destLogFolderPath, err := settings.GetDestFolderPath()
			if err != nil {
				return err
			}
			return indexFileCreator(destLogFolderPath, objects)
		},
		abortHandler: func(logName string) error {
			dstPathFolder, err := settings.GetDestFolderPath()
			if err != nil {
				return err
			}
			return os.Remove(path.Join(dstPathFolder, logName))
		},
	}
}

func configureEndTs(untilDT string) (*time.Time, error) {
	if untilDT != "" {
		dt, err := time.Parse(time.RFC3339, untilDT)
		if err != nil {
			return nil, err
		}
		return &dt, nil
	}
	return internal.ParseTS(BinlogEndTsSetting)
}

func FetchLogs(folder storage.Folder, backupUploadTime time.Time, untilDT string, needApply bool) error {
	endTS, err := configureEndTs(untilDT)
	if err != nil {
		return err
	}
	settings := BinlogFetchSettings{
		startTs:   backupUploadTime,
		endTS:     endTS,
		needApply: needApply,
	}
	handler := NewBinlogFetchHandler(settings)
	_, err = internal.FetchLogs(folder, settings, handler)
	return err
}

func getBackupUploadTime(folder storage.Folder, backup *internal.Backup) (time.Time, error) {
	var streamSentinel StreamSentinelDto
	err := internal.FetchStreamSentinel(backup, &streamSentinel)
	if err != nil {
		return time.Time{}, err
	}

	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}

	var backupUploadTime time.Time
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			backupUploadTime = binlog.GetLastModified()
		}
	}

	return backupUploadTime, nil
}

func isBinlogCreatedAfterEndTs(binlogTimestamp time.Time, endTS *time.Time) bool {
	return endTS != nil && binlogTimestamp.After(*endTS)
}

func createIndexFile(logsFolder string, fetchedBinlogs []storage.Object) error {
	indexFile, err := os.Create(filepath.Join(logsFolder, "binlogs_order"))
	if err != nil {
		return err
	}

	for _, binlogObject := range fetchedBinlogs {
		_, err = indexFile.WriteString(utility.TrimFileExtension(binlogObject.GetName()) + "\n")
		if err != nil {
			return err
		}
	}
	return indexFile.Close()
}

func HandleBinlogFetch(folder storage.Folder, backupName string, untilDT string, needApply bool) error {
	backup, err := internal.GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %+v\n", err)
	backupUploadTime, err := getBackupUploadTime(folder, backup)
	if err != nil {
		return err
	}
	return FetchLogs(folder, backupUploadTime, untilDT, needApply)
}
