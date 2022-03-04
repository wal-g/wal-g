package mysql

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
	GTIDArchived       string `json:"GtidArchived"`
}

//gocyclo:ignore
func HandleBinlogPush(uploader internal.UploaderProvider, untilBinlog string, checkGTIDs bool) {
	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

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
		tracelog.InfoLogger.Printf("fetched binlog archived GTID SET: %s\n", cache.GTIDArchived)
	}

	var filter gtidFilter
	if checkGTIDs {
		flavor, err := getMySQLFlavor(db)
		if flavor == "" || err != nil {
			flavor = mysql.MySQLFlavor
		}
		if flavor == mysql.MySQLFlavor {
			gtid, _ := mysql.ParseMysqlGTIDSet(cache.GTIDArchived)
			gtidArchived, _ := gtid.(*mysql.MysqlGTIDSet)
			filter = gtidFilter{
				BinlogsFolder: binlogsFolder,
				Flavor:        flavor,
				gtidArchived:  gtidArchived,
				lastGtidSeen:  nil,
			}
		}
	}

outer:
	for i := 0; i < len(binlogs); i++ {
		binlog := binlogs[i]

		tracelog.DebugLogger.Printf("Testing... %v\n", binlog)

		if binlog >= untilBinlog {
			tracelog.DebugLogger.Printf("Skip binlog %v (until check)\n", binlog)
			continue outer
		}

		if binlog <= cache.LastArchivedBinlog {
			tracelog.DebugLogger.Printf("Skip binlog %v (archived binlog check)\n", binlog)
			continue outer
		}

		if checkGTIDs && filter.isValid() {
			nextBinlog := ""
			if i < len(binlogs)-1 {
				nextBinlog = binlogs[i+1]
			}
			if !filter.shouldUpload(binlog, nextBinlog) {
				tracelog.DebugLogger.Printf("Skip binlog %v (gtid check)\n", binlog)
				// in fact this binlog had been uploaded before. Mark it as uploaded:
				cache.LastArchivedBinlog = binlog
				continue
			}
		}

		// Upload binlogs:
		err = archiveBinLog(uploader, binlogsFolder, binlog)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedBinlog = binlog
		if checkGTIDs && filter.isValid() {
			cache.GTIDArchived = filter.gtidArchived.String()
		}
		putCache(cache) // sync cache to disk from time to time
	}

	// Write Binlog Cache (even when no data uploaded, it will create file on first run)
	putCache(cache)

	// Write Binlog Sentinel
	binlogSentinelDto.GTIDArchived = cache.GTIDArchived
	tracelog.InfoLogger.Printf("Binlog sentinel: %s, cache: %+v", binlogSentinelDto.String(), cache)
	err = UploadBinlogSentinel(rootFolder, &binlogSentinelDto)
	tracelog.ErrorLogger.FatalOnError(err)
}

func getMySQLSortedBinlogs(db *sql.DB) ([]string, error) {
	var result []string
	// SHOW BINARY LOGS acquire binlog mutex and may hang while mysql is committing huge transactions
	// so we read binlog index from the disk with no locking
	row := db.QueryRow("SELECT @@log_bin_index")
	var binlogIndex string
	err := row.Scan(&binlogIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql variable: %w", err)
	}
	fh, err := os.Open(binlogIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to open binlog index: %w", err)
	}
	s := bufio.NewScanner(fh)
	for s.Scan() {
		binlog := path.Base(s.Text())
		result = append(result, binlog)
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

func archiveBinLog(uploader internal.UploaderProvider, dataDir string, binlog string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", binlog)

	filename := path.Join(dataDir, binlog)
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
		file, err = os.ReadFile(cacheFilename)
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
		err = os.WriteFile(cacheFilename, marshal, 0644)
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to write MySQL binlog cache file: %v\n", err)
		}
	}
}

type gtidFilter struct {
	BinlogsFolder string
	Flavor        string
	gtidArchived  *mysql.MysqlGTIDSet
	lastGtidSeen  *mysql.MysqlGTIDSet
}

func (u *gtidFilter) isValid() bool {
	if u.Flavor == "" {
		return false
	}
	if u.Flavor != mysql.MySQLFlavor {
		// MariaDB GTID Sets consists of: DomainID + ServerID + Sequence Number (64-bit unsigned integer)
		// It is not clear how it handles gaps in SequenceNumbers, so for safety reasons skip this check
		return false
	}
	return true
}

func (u *gtidFilter) shouldUpload(binlog, nextBinlog string) bool {
	// nextPreviousGTIDs is 'GTIDs_executed at the end of current binary log file'
	nextPreviousGTIDs, err := GetBinlogPreviousGTIDs(path.Join(u.BinlogsFolder, nextBinlog), u.Flavor)
	if err != nil {
		tracelog.InfoLogger.Printf("Cannot extract PREVIOUS_GTIDS event from binlog %s. Upload it. (gtid check)\n", binlog)
		return true
	}

	if u.gtidArchived == nil {
		tracelog.DebugLogger.Printf("Cannot extract set of uploaded binlogs from cache\n")
		// continue uploading even when we cannot read uploadedGTIDs
		u.gtidArchived = nextPreviousGTIDs
		u.lastGtidSeen = nextPreviousGTIDs
		return true
	}

	if u.lastGtidSeen == nil {
		gtidSetBeforeCurrentBinlog, err := GetBinlogPreviousGTIDs(path.Join(u.BinlogsFolder, binlog), u.Flavor)
		if err != nil {
			tracelog.InfoLogger.Printf("Cannot extract PREVIOUS_GTIDS event from current binlog %s. Upload it. (gtid check)\n", binlog)
			u.lastGtidSeen = nextPreviousGTIDs
			return true
		}
		tracelog.DebugLogger.Printf("Binlog %s is the first binlog that we seen by GTID-checker in this run. (gtid check)\n", binlog)
		u.lastGtidSeen = gtidSetBeforeCurrentBinlog
	}

	currentBinlogGTIDSet := nextPreviousGTIDs.Clone().(*mysql.MysqlGTIDSet)
	err = currentBinlogGTIDSet.Minus(*u.lastGtidSeen)
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot subtract GTIDs: %v (gtid check)\n", err)
		return true // math is broken. upload binlog
	}

	// when we know that _next_ binlog's PreviousGTID already uploaded we can safely skip _current_ binlog
	if u.gtidArchived.Contain(currentBinlogGTIDSet) {
		tracelog.InfoLogger.Printf("Binlog %v with GTID Set %s already archived (gtid check)\n", binlog, currentBinlogGTIDSet.String())
		u.lastGtidSeen = nextPreviousGTIDs
		return false
	}

	err = u.gtidArchived.Add(*currentBinlogGTIDSet)
	if err != nil {
		tracelog.WarningLogger.Printf("Cannot merge GTIDs: %v (gtid check)\n", err)
		return true // math is broken. upload binlog
	}
	tracelog.InfoLogger.Printf("Should upload binlog %s with GTID set: %s (gtid check)\n", binlog, currentBinlogGTIDSet.String())
	u.lastGtidSeen = nextPreviousGTIDs
	return true
}
