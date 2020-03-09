package mysql

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

func HandleBinlogPush(uploader *internal.Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(BinlogPath)

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogsFolder, err := getMySQLBinlogsFolder(db)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLSortedBinlogs(db)
	tracelog.ErrorLogger.FatalOnError(err)

	for _, binLog := range binlogs {
		err = tryArchiveBinLog(uploader, path.Join(binlogsFolder, binLog), binLog)
		tracelog.ErrorLogger.FatalOnError(err)
	}
}

func getMySQLSortedBinlogs(db *sql.DB) ([]string, error) {
	var result []string

	currentBinlog := getMySQLCurrentBinlogFile(db)

	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer utility.LoggedClose(rows, "")
	for rows.Next() {
		var logFinName string
		var size uint32
		err = scanToMap(rows, map[string]interface{}{"Log_name": &logFinName, "File_size": &size})
		if err != nil {
			return nil, err
		}
		if logFinName < currentBinlog {
			result = append(result, logFinName)
		}
	}
	sort.Strings(result)
	return result, nil
}

func getMySQLBinlogsFolder(db *sql.DB) (string, error) {
	row := db.QueryRow("SHOW VARIABLES LIKE 'log_bin_basename'")
	var nonce, logBinBasename string
	err := row.Scan(&nonce, &logBinBasename)
	if err != nil {
		return "", err
	}
	return path.Dir(logBinBasename), nil
}

func tryArchiveBinLog(uploader *internal.Uploader, filename string, binLog string) error {
	if binLog <= getLastArchivedBinlog() {
		tracelog.InfoLogger.Printf("Binlog %v already archived\n", binLog)
		return nil
	}
	tracelog.InfoLogger.Printf("Archiving %v\n", binLog)

	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	setLastArchivedBinlog(binLog)
	return nil
}

func getLastArchivedBinlog() string {
	var cache LogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, BinlogCacheFileName)
		var file []byte
		file, err = ioutil.ReadFile(cacheFilename)
		if err == nil {
			err = json.Unmarshal(file, &cache)
			if err == nil {
				return cache.LastArchivedBinlog
			}
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL binlog cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return ""
}

func setLastArchivedBinlog(binlogFileName string) {
	var cache LogsCache
	var cacheFilename string

	usr, err := user.Current()
	if err == nil {
		cacheFilename = filepath.Join(usr.HomeDir, BinlogCacheFileName)
		var file []byte
		file, err = ioutil.ReadFile(cacheFilename)
		// here we ignore whatever error can occur
		if err == nil {
			_ = json.Unmarshal(file, &cache)
		}
	}
	if err != nil && !os.IsNotExist(err) {
		tracelog.ErrorLogger.Printf("Failed to read MySQL binlog cache file: %v\n", err)
	}

	cache.LastArchivedBinlog = binlogFileName

	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		err = ioutil.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to write MySQL binlog cache file: %v\n", err)
		}
	}
}
