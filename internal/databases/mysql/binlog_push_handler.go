package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
	GTIDArchived       string `json:"gtid_archived"`
}

//gocyclo:ignore
func HandleBinlogPush(uploader *internal.Uploader, untilBinlog string, checkGTIDs bool) {
	rootFolder := uploader.UploadingFolder
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(BinlogPath)

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogsFolder, err := getMySQLBinlogsFolder(db)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLSortedBinlogs(db)
	tracelog.ErrorLogger.FatalOnError(err)

	if untilBinlog == "" || untilBinlog > getMySQLCurrentBinlogFileLocal(db) {
		untilBinlog = getMySQLCurrentBinlogFileLocal(db)
	}

	var binlogSentinelDto BinlogSentinelDto
	err = FetchBinlogSentinel(rootFolder, &binlogSentinelDto)
	// copy MySQLBinlogSentinel to cache:
	cache := getCache()
	if err == nil && binlogSentinelDto.GTIDArchived != "" {
		cache.GTIDArchived = binlogSentinelDto.GTIDArchived
	}

	for i := 0; i < len(binlogs); i++ {
		binLog := binlogs[i]
		nextBinLog := ""
		if i < len(binlogs)-1 {
			nextBinLog = binlogs[i+1]
		}

		tracelog.InfoLogger.Printf("Testing... %v\n", binLog)

		if binLog <= cache.LastArchivedBinlog {
			tracelog.DebugLogger.Printf("Binlog %v already archived (filename check)\n", binLog)
			continue
		}

		if binLog >= untilBinlog {
			continue
		}

		var nextPreviousGTIDs mysql.GTIDSet
		if nextBinLog != "" {
			// nextPreviousGTIDs is 'GTIDs_executed in the current binary log file'
			nextPreviousGTIDs, err = peekPreviousGTIDs(path.Join(binlogsFolder, nextBinLog))
			if err != nil {
				tracelog.InfoLogger.Printf("cannot extract PREVIOUS_GTIDS event from binlog %s\n", binLog)
				// continue uploading even when we cannot parse next binlog
			}
			uploadedGTIDs, err := mysql.ParseMysqlGTIDSet(cache.GTIDArchived)
			if err != nil {
				tracelog.DebugLogger.Printf("cannot extract set of uploaded binlgos from cache\n")
				// continue uploading even when we cannot read uploadedGTIDs
			} else if checkGTIDs {
				// when we know that _next_ binlog's PreviousGTID already uploaded we can safely skip _current_ binlog
				if uploadedGTIDs.String() != "" && uploadedGTIDs.Contain(nextPreviousGTIDs) {
					tracelog.InfoLogger.Printf("Binlog %v already archived (GTID check)\n", binLog)
					continue
				}
			}
		}

		err = archiveBinLog(uploader, binlogsFolder, binLog)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedBinlog = binLog
		if nextPreviousGTIDs != nil {
			cache.GTIDArchived = nextPreviousGTIDs.String()
		}
		putCache(cache)
	}

	// Write Binlog Sentinel
	binlogSentinelDto.GTIDArchived = cache.GTIDArchived

	tracelog.InfoLogger.Printf("Binlog sentinel: %s", binlogSentinelDto.String())
	err = UploadBinlgoSentinel(rootFolder, &binlogSentinelDto)
	tracelog.ErrorLogger.FatalOnError(err)
}

func getMySQLSortedBinlogs(db *sql.DB) ([]string, error) {
	var result []string

	rows, err := db.Query("SHOW BINARY LOGS")
	if err != nil {
		return nil, err
	}
	defer utility.LoggedClose(rows, "")
	for rows.Next() {
		var logFinName string
		var size uint64
		err = utility.ScanToMap(rows, map[string]interface{}{"Log_name": &logFinName, "File_size": &size})
		if err != nil {
			return nil, err
		}
		result = append(result, logFinName)
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

func archiveBinLog(uploader *internal.Uploader, dataDir string, binLog string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", binLog)

	filename := path.Join(dataDir, binLog)
	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not upload '%s'\n", filename)
	}

	return nil
}

func getCache() LogsCache {
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
				return cache
			}
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL binlog cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return LogsCache{}
}

func putCache(cache LogsCache) {
	var cacheFilename string

	usr, err := user.Current()
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get current user homedir: %v\n", err)
	}
	cacheFilename = filepath.Join(usr.HomeDir, BinlogCacheFileName)
	if err != nil && !os.IsNotExist(err) {
		tracelog.ErrorLogger.Printf("Failed to read MySQL binlog cache file: %v\n", err)
	}

	marshal, err := json.Marshal(&cache)
	if err == nil && len(cacheFilename) > 0 {
		err = ioutil.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to write MySQL binlog cache file: %v\n", err)
		}
	}
}

func peekPreviousGTIDs(filename string) (mysql.GTIDSet, error) {
	var found bool
	previousGTID := &replication.PreviousGTIDsEvent{}

	parser := replication.NewBinlogParser()
	parser.SetFlavor("mysql")
	parser.SetVerifyChecksum(false) // the faster, the better
	parser.SetRawMode(true)         // choose events to parse manually
	err := parser.ParseFile(filename, 0, func(event *replication.BinlogEvent) error {
		if event.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			err := previousGTID.Decode(event.RawData[19:])
			if err != nil {
				return err
			}
			found = true
			return fmt.Errorf("shallow file read finished")
		}
		return nil
	})

	if err != nil && !found {
		result, _ := mysql.ParseMysqlGTIDSet("")
		return result, errors.Wrapf(err, "binlog-push: could not parse binlog file '%s'\n", filename)
	}

	return mysql.ParseMysqlGTIDSet(previousGTID.GTIDSets)
}
