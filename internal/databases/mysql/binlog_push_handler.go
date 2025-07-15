package mysql

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

//gocyclo:ignore
//nolint:funlen
func HandleBinlogPush(uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")

	binlogsFolder, err := getMySQLBinlogsFolder(db)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(db)
	tracelog.ErrorLogger.FatalOnError(err)

	lastBinlog := lastOrDefault(binlogs, "")
	if untilBinlog == "" || BinlogNum(untilBinlog) > BinlogNum(lastBinlog) {
		untilBinlog = lastBinlog
	}

	var binlogSentinelDto BinlogSentinelDto
	err = FetchBinlogSentinel(rootFolder, &binlogSentinelDto)
	if err == nil && binlogSentinelDto.GTIDArchived != "" {
		tracelog.InfoLogger.Printf("fetched binlog archived GTID SET: %s\n", binlogSentinelDto.GTIDArchived)
	}
	cache := getCache()
	if len(binlogs) > 0 && cache.LastArchivedBinlog != "" {
		if BinlogPrefix(binlogs[0]) != BinlogPrefix(cache.LastArchivedBinlog) ||
			BinlogNum(binlogs[len(binlogs)-1]) < BinlogNum(cache.LastArchivedBinlog) {
			tracelog.WarningLogger.Printf("binlog was reset or naming (%s => %s), clearing cache",
				cache.LastArchivedBinlog, binlogs[0])
			cache = LogsCache{}
		}
	}

	var filter gtidFilter
	if checkGTIDs {
		flavor, err := getMySQLFlavor(db)
		tracelog.ErrorLogger.FatalOnError(err)

		switch flavor {
		case mysql.MySQLFlavor:
			gtid, _ := mysql.ParseMysqlGTIDSet(binlogSentinelDto.GTIDArchived)
			gtidArchived, _ := gtid.(*mysql.MysqlGTIDSet)
			filter = gtidFilter{
				BinlogsFolder: binlogsFolder,
				Flavor:        flavor,
				gtidArchived:  gtidArchived,
				lastGtidSeen:  nil,
			}
		default:
			tracelog.ErrorLogger.Fatalf("Unsupported flavor type: %s. Disable WALG_MYSQL_CHECK_GTIDS for current database.", flavor)
		}
	}

	hadUploadsInThisRun := false
	for i := 0; i < len(binlogs); i++ {
		binlog := binlogs[i]

		tracelog.DebugLogger.Printf("Testing... %v\n", binlog)

		if untilBinlog != "" && BinlogNum(binlog) >= BinlogNum(untilBinlog) {
			tracelog.DebugLogger.Printf("Skip binlog %v (until check)\n", binlog)
			continue
		}

		if cache.LastArchivedBinlog != "" && BinlogNum(binlog) <= BinlogNum(cache.LastArchivedBinlog) {
			tracelog.DebugLogger.Printf("Skip binlog %v (archived binlog check)\n", binlog)
			continue
		}

		if checkGTIDs && filter.isValid() {
			nextBinlog := ""
			if i < len(binlogs)-1 {
				nextBinlog = binlogs[i+1]
			}
			shouldUpload := filter.shouldUpload(binlog, nextBinlog)
			if !hadUploadsInThisRun && !shouldUpload {
				tracelog.DebugLogger.Printf("Skip binlog %v (gtid check)\n", binlog)
				// in fact this binlog had been uploaded before. Mark it as uploaded:
				cache.LastArchivedBinlog = binlog
				continue
			}

			// During PITR WAL-G will apply binlogs one-by-one from oldest to newest
			// (based on upload timestamp) without checking GTID sets.
			// It means that during upload phase it is not possible to fill the gaps
			// in GTID sets (because it will break during PITR phase).
			// So, for safety reasons
			// we will upload all other binlogs after uploading single binlog.
			hadUploadsInThisRun = true
		}

		// Upload binlogs:
		err = archiveBinLog(uploader, binlogsFolder, binlog)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedBinlog = binlog
		putCache(cache)

		// Write Binlog Sentinel
		if checkGTIDs && filter.isValid() {
			binlogSentinelDto.GTIDArchived = filter.gtidArchived.String()
			tracelog.InfoLogger.Printf("Uploading binlog sentinel: %s", binlogSentinelDto)
			err := UploadBinlogSentinel(rootFolder, &binlogSentinelDto)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}

	// Write Binlog Cache (even when no data uploaded, it will create file on first run)
	putCache(cache)
}

func getMySQLBinlogs(db *sql.DB) ([]string, error) {
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
	// binlogs in index files are already sorted actually, so we don't need to sort them again
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

func archiveBinLog(uploader internal.Uploader, dataDir string, binlog string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", binlog)

	filename := path.Join(dataDir, binlog)
	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(context.Background(), walFile)
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
	if nextBinlog == "" {
		// it is better to skip this binlog rather than have gap in binlog sentinel GTID-set
		tracelog.DebugLogger.Printf("Cannot extract PREVIOUS_GTIDS event - no 'next' binlog found. Skip it for now. (gtid check)\n")
		return false
	}
	// nextPreviousGTIDs is 'GTIDs_executed at the end of current binary log file'
	_nextPreviousGTIDs, err := GetBinlogPreviousGTIDs(path.Join(u.BinlogsFolder, nextBinlog), u.Flavor)
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Cannot extract PREVIOUS_GTIDS event from current binlog %s, next %s (caused by %v). Upload it. (gtid check)\n",
			binlog, nextBinlog, err)
		return true
	}
	nextPreviousGTIDs := _nextPreviousGTIDs.(*mysql.MysqlGTIDSet)

	if u.gtidArchived == nil || u.gtidArchived.String() == "" {
		tracelog.DebugLogger.Printf("Cannot extract set of uploaded binlogs from cache\n")
		// continue uploading even when we cannot read uploadedGTIDs
		u.gtidArchived = nextPreviousGTIDs
		u.lastGtidSeen = nextPreviousGTIDs
		return true
	}

	if u.lastGtidSeen == nil {
		gtidSetBeforeCurrentBinlog, err := GetBinlogPreviousGTIDs(path.Join(u.BinlogsFolder, binlog), u.Flavor)
		if err != nil {
			tracelog.InfoLogger.Printf(
				"Cannot extract PREVIOUS_GTIDS event from current binlog %s, next %s (caused by %v). Upload it. (gtid check)\n",
				binlog, nextBinlog, err)
			u.lastGtidSeen = nextPreviousGTIDs
			return true
		}
		tracelog.DebugLogger.Printf("Binlog %s is the first binlog that we seen by GTID-checker in this run. (gtid check)\n", binlog)
		u.lastGtidSeen = gtidSetBeforeCurrentBinlog.(*mysql.MysqlGTIDSet)
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

func lastOrDefault(data []string, defaultValue string) string {
	if len(data) > 0 {
		return data[len(data)-1]
	}
	return defaultValue
}
