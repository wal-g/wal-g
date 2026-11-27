package mysql

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog     string `json:"LastArchivedBinlog"`
	LastArchivedBinlogSize int64  `json:"LastArchivedBinlogSize"`
}

// binlogGtidFilter decides, based on a database's GTID state, whether a binlog
// needs to be uploaded again and which GTID set has been archived so far.
// Implementations exist per MySQL flavor (mysqlGtidFilter, mariadbGtidFilter).
type binlogGtidFilter interface {
	// isValid returns true if the filter is properly configured for the current flavor.
	isValid() bool
	// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
	shouldUpload(binlog, nextBinlog string) bool
	// getArchivedGTIDString returns the string representation of the archived GTID checkpoint for the current flavor.
	getArchivedGTIDString() string
}

//gocyclo:ignore
//nolint:funlen
func HandleBinlogPush(ctx context.Context, uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	rootFolder := uploader.Folder()
	uploader.ChangeDirectory(BinlogPath)

	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")

	binlogsFolder, err := getMySQLBinlogsFolder(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	lastBinlog := lastOrDefault(binlogs, "")
	if untilBinlog != "" && BinlogNum(untilBinlog) > BinlogNum(lastBinlog) {
		untilBinlog = lastBinlog
	}

	var binlogSentinelDto BinlogSentinelDto
	err = FetchBinlogSentinel(ctx, rootFolder, &binlogSentinelDto)
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

	var filter binlogGtidFilter
	if checkGTIDs {
		flavor, err := getMySQLFlavor(conn)
		tracelog.ErrorLogger.FatalOnError(err)

		switch flavor {
		case mysql.MySQLFlavor:
			var gtidArchived *mysql.MysqlGTIDSet
			gtid, err := mysql.ParseMysqlGTIDSet(binlogSentinelDto.GTIDArchived)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to parse MySQL GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
					binlogSentinelDto.GTIDArchived, err)
			} else {
				var ok bool
				gtidArchived, ok = gtid.(*mysql.MysqlGTIDSet)
				if !ok {
					tracelog.WarningLogger.Printf(
						"Failed to convert GTID to MysqlGTIDSet type. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.")
					gtidArchived = nil
				}
			}
			filter = &mysqlGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  gtidArchived,
				lastGtidSeen:  nil,
			}
		case mysql.MariaDBFlavor:
			var mariadbGTID *mysql.MariadbGTIDSet
			if binlogSentinelDto.GTIDArchived != "" {
				gtid, err := mysql.ParseMariadbGTIDSet(binlogSentinelDto.GTIDArchived)
				if err != nil {
					tracelog.WarningLogger.Printf(
						"Failed to parse MariaDB GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
						binlogSentinelDto.GTIDArchived, err)
				} else {
					var ok bool
					mariadbGTID, ok = gtid.(*mysql.MariadbGTIDSet)
					if !ok {
						tracelog.WarningLogger.Printf(
							"Failed to convert GTID to MariadbGTIDSet type. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.")
						mariadbGTID = nil
					}
				}
			}
			filter = &mariadbGtidFilter{
				binlogsFolder: binlogsFolder,
				flavor:        flavor,
				gtidArchived:  mariadbGTID,
			}
			tracelog.InfoLogger.Printf("Using MariaDB GTID filter for binlog push")
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

		if cache.LastArchivedBinlog != "" && (
			BinlogNum(binlog) < BinlogNum(cache.LastArchivedBinlog) || (
				BinlogNum(binlog) == BinlogNum(cache.LastArchivedBinlog) &&
				getBinlogSize(binlogsFolder, binlog) <= cache.LastArchivedBinlogSize)) {
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
				cache.LastArchivedBinlogSize = getBinlogSize(binlogsFolder, binlog)
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
		err = archiveBinLog(ctx, uploader, binlogsFolder, binlog)
		tracelog.ErrorLogger.FatalOnError(err)

		cache.LastArchivedBinlog = binlog
		cache.LastArchivedBinlogSize = getBinlogSize(binlogsFolder, binlog)
		putCache(cache)

		// Write Binlog Sentinel
		if checkGTIDs && filter.isValid() {
			binlogSentinelDto.GTIDArchived = filter.getArchivedGTIDString()
			tracelog.InfoLogger.Printf("Uploading binlog sentinel: %s", binlogSentinelDto)
			err := UploadBinlogSentinel(ctx, rootFolder, &binlogSentinelDto)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}

	// Write Binlog Cache (even when no data uploaded, it will create file on first run)
	putCache(cache)
}

func getMySQLBinlogs(conn *client.Conn) ([]string, error) {
	var result []string
	// SHOW BINARY LOGS acquire binlog mutex and may hang while mysql is committing huge transactions
	// so we read binlog index from the disk with no locking
	r, err := conn.Execute("SELECT @@log_bin_index")
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql variable: %w", err)
	}
	defer r.Close()
	binlogIndex, err := r.GetString(0, 0)
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

func getMySQLBinlogsFolder(conn *client.Conn) (string, error) {
	r, err := conn.Execute("SHOW VARIABLES LIKE 'log_bin_basename'")
	if err != nil {
		return "", err
	}
	defer r.Close()
	logBinBasename, err := r.GetString(0, 1)
	if err != nil {
		return "", err
	}
	return path.Dir(logBinBasename), nil
}

func archiveBinLog(ctx context.Context, uploader internal.Uploader, dataDir string, binlog string) error {
	tracelog.InfoLogger.Printf("Archiving %v\n", binlog)

	filename := path.Join(dataDir, binlog)
	walFile, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", filename)
	}
	defer utility.LoggedClose(walFile, "")
	err = uploader.UploadFile(ctx, walFile)
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

func lastOrDefault(data []string, defaultValue string) string {
	if len(data) > 0 {
		return data[len(data)-1]
	}
	return defaultValue
}

func getBinlogSize(binlogsFolder string, binlog string) int64 {
	fi, err := os.Stat(path.Join(binlogsFolder, binlog))
	if err != nil {
		tracelog.InfoLogger.Printf("Cannot stat binlog %s: %v", binlog, err)
		return 0
	}
	return fi.Size()
}
