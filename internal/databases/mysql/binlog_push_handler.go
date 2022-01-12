package mysql

import (
	"bufio"
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
	}

	var filters []statefulBinlogFilter
	filters = append(filters, &untilBinlogFilter{Until: untilBinlog}, &archivedBinlogFilter{})
	if checkGTIDs {
		flavor, err := getFlavor(db)
		if flavor == "" || err != nil {
			flavor = mysql.MySQLFlavor
		}
		if flavor == mysql.MySQLFlavor {
			filters = append(filters, &gtidFilter{BinlogsFolder: binlogsFolder, Flavor: flavor})
		}
	}
	for _, filter := range filters {
		filter.init(cache)
	}

outer:
	for i := 0; i < len(binlogs); i++ {
		binLog := binlogs[i]

		tracelog.DebugLogger.Printf("Testing... %v\n", binLog)
		for _, filter := range filters {
			nextBinLog := ""
			if i < len(binlogs)-1 {
				nextBinLog = binlogs[i+1]
			}
			if !filter.test(binLog, nextBinLog) {
				tracelog.DebugLogger.Printf("Skip binlog %v (%s check)\n", binLog, filter.name())
				continue outer
			}
		}

		// Upload binlogs:
		err = archiveBinLog(uploader, binlogsFolder, binLog)
		tracelog.ErrorLogger.FatalOnError(err)

		for _, filter := range filters {
			filter.onUpload(&cache)
		}
		putCache(cache)
	}

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

func archiveBinLog(uploader internal.UploaderProvider, dataDir string, binLog string) error {
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

type statefulBinlogFilter interface {
	name() string
	init(LogsCache)
	onUpload(*LogsCache)
	test(binlog, nextBinlog string) bool
}

type untilBinlogFilter struct {
	Until string
}

var _ statefulBinlogFilter = &untilBinlogFilter{}

func (u *untilBinlogFilter) init(LogsCache) {}
func (u *untilBinlogFilter) name() string {
	return "until"
}
func (u *untilBinlogFilter) onUpload(*LogsCache) {}
func (u *untilBinlogFilter) test(binlog, _ string) bool {
	return binlog < u.Until
}

type archivedBinlogFilter struct {
	lastArchived string
	lastTested   string
}

var _ statefulBinlogFilter = &archivedBinlogFilter{}

func (u *archivedBinlogFilter) init(data LogsCache) {
	u.lastTested = data.LastArchivedBinlog
	u.lastArchived = data.LastArchivedBinlog
}
func (u *archivedBinlogFilter) name() string {
	return "archived binlog"
}
func (u *archivedBinlogFilter) onUpload(data *LogsCache) {
	data.LastArchivedBinlog = u.lastTested
}
func (u *archivedBinlogFilter) test(binlog, _ string) bool {
	if binlog > u.lastArchived {
		u.lastTested = binlog
		return true
	}
	return false
}

type gtidFilter struct {
	BinlogsFolder string
	Flavor        string
	gtidArchived  *mysql.MysqlGTIDSet
	lastGtidSeen  *mysql.MysqlGTIDSet
}

var _ statefulBinlogFilter = &gtidFilter{}

func (u *gtidFilter) init(data LogsCache) {
	gtid, _ := mysql.ParseMysqlGTIDSet(data.GTIDArchived)
	u.gtidArchived, _ = gtid.(*mysql.MysqlGTIDSet)
	u.lastGtidSeen = nil
}
func (u *gtidFilter) name() string {
	return "gtid"
}
func (u *gtidFilter) onUpload(data *LogsCache) {
	data.GTIDArchived = u.gtidArchived.String()
}
func (u *gtidFilter) test(binlog, nextBinlog string) bool {
	if u.Flavor != mysql.MySQLFlavor {
		// MariaDB GTID Sets consists of: DomainID + ServerID + Sequence Number (64-bit unsigned integer)
		// It is not clear how it handles gaps in SequenceNumbers, so for safety reasons skip this check
		return true
	}

	// nextPreviousGTIDs is 'GTIDs_executed in the current binary log file'
	nextPreviousGTIDs, err := peekPreviousMysqlGTIDs(path.Join(u.BinlogsFolder, nextBinlog), u.Flavor)
	if err != nil {
		tracelog.InfoLogger.Printf("cannot extract PREVIOUS_GTIDS event from binlog %s\n", binlog)
		// continue uploading even when we cannot parse next binlog
	}

	if u.gtidArchived == nil {
		tracelog.DebugLogger.Printf("cannot extract set of uploaded binlgs from cache\n")
		// continue uploading even when we cannot read uploadedGTIDs
		u.gtidArchived = nextPreviousGTIDs
		u.lastGtidSeen = nextPreviousGTIDs
		return true
	}
	// when we know that _next_ binlog's PreviousGTID already uploaded we can safely skip _current_ binlog
	if u.gtidArchived.Contain(nextPreviousGTIDs) {
		tracelog.InfoLogger.Printf("Binlog %v already archived (%s check)\n", binlog, u.name())
		u.lastGtidSeen = nextPreviousGTIDs
		return false
	}

	if u.lastGtidSeen != nil {
		currentBinlogGTIDSet := nextPreviousGTIDs.Clone().(*mysql.MysqlGTIDSet)
		err = currentBinlogGTIDSet.Minus(*u.lastGtidSeen)
		if err != nil {
			tracelog.InfoLogger.Printf("Cannot subtract GTIDs: %v (%s check)\n", err, u.name())
			return true // math is brokern. upload binlog
		}
		err = u.gtidArchived.Add(*currentBinlogGTIDSet)
		if err != nil {
			tracelog.InfoLogger.Printf("Cannot merge GTIDs: %v (%s check)\n", err, u.name())
			return true // math is brokern. upload binlog
		}
	}
	u.lastGtidSeen = nextPreviousGTIDs
	return true
}

func peekPreviousMysqlGTIDs(filename string, flavor string) (*mysql.MysqlGTIDSet, error) {
	var found bool
	previousGTID := &replication.PreviousGTIDsEvent{}

	parser := replication.NewBinlogParser()
	parser.SetFlavor(flavor)
	parser.SetVerifyChecksum(false) // the faster, the better
	parser.SetRawMode(true)         // choose events to parse manually
	err := parser.ParseFile(filename, 0, func(event *replication.BinlogEvent) error {
		if event.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			err := previousGTID.Decode(event.RawData[replication.EventHeaderSize:])
			if err != nil {
				return err
			}
			found = true
			return fmt.Errorf("shallow file read finished")
		}
		return nil
	})

	if err != nil && !found {
		return nil, errors.Wrapf(err, "binlog-push: could not parse binlog file '%s'\n", filename)
	}

	res, err := mysql.ParseMysqlGTIDSet(previousGTID.GTIDSets)
	if err != nil {
		return nil, err
	}
	result, ok := res.(*mysql.MysqlGTIDSet)
	if !ok {
		tracelog.ErrorLogger.Fatalf("cannot cast nextPreviousGTIDs to MysqlGTIDSet. Should never be here. Actual type: %T\n", res)
	}
	return result, nil
}
