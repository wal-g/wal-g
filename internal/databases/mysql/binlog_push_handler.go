package mysql

import (
	"database/sql"
	"encoding/json"
	"github.com/spf13/viper"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

func HandleBinlogPush(uploader *Uploader) {
	if !viper.IsSet(BinlogSrcSetting) {
		tracelog.ErrorLogger.FatalError(internal.NewUnsetRequiredSettingError(BinlogSrcSetting))
	}
	binlogsFolder := viper.GetString(BinlogSrcSetting)
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(BinlogPath)
	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogs := getMySQLSortedBinlogs(db)

	for _, binLog := range binlogs {
		err = tryArchiveBinLog(uploader, path.Join(binlogsFolder, binLog), binLog)
		tracelog.ErrorLogger.FatalOnError(err)
	}
}

func getMySQLSortedBinlogs(db *sql.DB) []string {
	var result []string

	currentBinlog := getMySQLCurrentBinlogFile(db)

	rows, err := db.Query("SHOW BINARY LOGS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	for rows.Next() {
		var logFinName string
		var size uint32
		err = scanToMap(rows, map[string]interface{}{"Log_name": &logFinName, "File_size": &size})
		tracelog.ErrorLogger.FatalOnError(err)
		if logFinName < currentBinlog {
			result = append(result, logFinName)
		}
	}
	sort.Strings(result)
	return result
}

func tryArchiveBinLog(uploader *Uploader, filename string, binLog string) error {
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
	err = uploader.UploadWalFile(walFile)
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
		tracelog.InfoLogger.Println("MySQL cache does not exist")
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
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}

	cache.LastArchivedBinlog = binlogFileName

	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		err = ioutil.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("%+v\n", err)
		}
	}
}
