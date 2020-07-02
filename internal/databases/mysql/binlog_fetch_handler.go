package mysql

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
	"os"
	"path"
	"path/filepath"
)

type indexHandler struct {
	dstDir  string
	binlogs []string
}

func newIndexHandler(dstDir string) *indexHandler {
	ih := new(indexHandler)
	ih.dstDir = dstDir
	return ih
}

func (ih *indexHandler) handleBinlog(binlogPath string) error {
	ih.binlogs = append(ih.binlogs, path.Base(binlogPath))
	return nil
}

func (ih *indexHandler) createIndexFile() error {
	indexFile, err := os.Create(filepath.Join(ih.dstDir, "binlogs_order"))
	if err != nil {
		return err
	}
	defer indexFile.Close()
	for _, binlog := range ih.binlogs {
		_, err = indexFile.WriteString(binlog + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func HandleBinlogFetch(folder storage.Folder, backupName string, untilDT string) {
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Unable to get backup %v", err)

	startTs, err := getBinlogStartTs(folder, backup)
	tracelog.ErrorLogger.FatalOnError(err)

	endTs, err := configureEndTs(untilDT)
	tracelog.ErrorLogger.FatalOnError(err)

	dstDir, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	handler := newIndexHandler(dstDir)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	err = fetchLogs(folder, dstDir, startTs, endTs, handler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = handler.createIndexFile()
	tracelog.ErrorLogger.FatalfOnError("Failed to create binlog index file: %v", err)
}
