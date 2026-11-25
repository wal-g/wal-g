package mysql

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const BinlogCacheFileName = ".walg_mysql_binlogs_cache"

type LogsCache struct {
	LastArchivedBinlog string `json:"LastArchivedBinlog"`
}

// BinlogUploadContext holds shared settings and storage handles for one binlog-push invocation.
type BinlogUploadContext struct {
	Uploader      internal.Uploader
	RootFolder    storage.Folder
	BinlogsFolder string
	Flavor        string
	GTIDPurged    mysql.GTIDSet
	CheckGTIDs    bool
	TrackGaps     bool
	CacheFileName string
	UntilBinlog   string
}

// binlogGtidFilter decides, based on gtid_archived, whether a binlog needs to be uploaded.
// Implementations exist per MySQL flavor (mysqlGtidFilter, mariadbGtidFilter).
type binlogGtidFilter interface {
	// isValid returns true if the filter is properly configured for the current flavor.
	isValid() bool
	// shouldUpload determines if a binlog should be uploaded based on GTID checkpoint comparison.
	shouldUpload(binlog, nextBinlog string) bool
}

func HandleBinlogPush(ctx context.Context, uploader internal.Uploader, untilBinlog string, checkGTIDs bool) {
	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")

	binlogsFolder, err := getMySQLBinlogsFolder(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	binlogs, err := getMySQLBinlogs(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	flavor, err := getMySQLFlavor(conn)
	tracelog.ErrorLogger.FatalOnError(err)

	trackGaps, err := conf.GetBoolSettingDefault(conf.MysqlBinlogGapTracking, false)
	tracelog.ErrorLogger.FatalOnError(err)

	var gtidPurged mysql.GTIDSet
	if trackGaps {
		gtidPurged, err = getMySQLGTIDPurged(conn, flavor)
		tracelog.ErrorLogger.FatalOnError(err)
	}

	usr, err := user.Current()
	tracelog.ErrorLogger.FatalOnError(err)
	cacheFileName := filepath.Join(usr.HomeDir, BinlogCacheFileName)

	uploadCtx := &BinlogUploadContext{
		Uploader:      uploader,
		BinlogsFolder: binlogsFolder,
		Flavor:        flavor,
		GTIDPurged:    gtidPurged,
		CheckGTIDs:    checkGTIDs,
		TrackGaps:     trackGaps,
		CacheFileName: cacheFileName,
		UntilBinlog:   untilBinlog,
	}
	err = pushBinlogs(ctx, uploadCtx, binlogs)
	tracelog.ErrorLogger.FatalOnError(err)
}

//gocyclo:ignore
//nolint:funlen
func pushBinlogs(ctx context.Context, uploadCtx *BinlogUploadContext, binlogs []string) error {
	uploader := uploadCtx.Uploader
	binlogsFolder := uploadCtx.BinlogsFolder
	flavor := uploadCtx.Flavor

	rootFolder := uploader.Folder()
	uploadCtx.RootFolder = rootFolder
	uploader.ChangeDirectory(BinlogPath)

	binlogSentinelDto, err := readBinlogSentinel(ctx, rootFolder)
	if err != nil {
		return err
	}
	cache := getCache(uploadCtx.CacheFileName)
	if len(binlogs) > 0 && cache.LastArchivedBinlog != "" {
		if BinlogPrefix(binlogs[0]) != BinlogPrefix(cache.LastArchivedBinlog) ||
			BinlogNum(binlogs[len(binlogs)-1]) < BinlogNum(cache.LastArchivedBinlog) {
			tracelog.WarningLogger.Printf("binlog was reset or naming (%s => %s), clearing cache",
				cache.LastArchivedBinlog, binlogs[0])
			cache = LogsCache{}
		}
	}

	var filter binlogGtidFilter
	if uploadCtx.CheckGTIDs {
		switch flavor {
		case mysql.MySQLFlavor:
			var gtidArchived *mysql.MysqlGTIDSet
			gtid, err := mysql.ParseMysqlGTIDSet(binlogSentinelDto.GTIDArchived)
			if err != nil {
				tracelog.WarningLogger.Printf(
					"Failed to parse MySQL GTID set '%s': %v. Uploading all binlogs and rebuilding the archived GTID checkpoint from scratch.",
					binlogSentinelDto.GTIDArchived, err)
				binlogSentinelDto.GTIDArchived = ""
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
					binlogSentinelDto.GTIDArchived = ""
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
			return fmt.Errorf("unsupported flavor type: %s. Disable WALG_MYSQL_CHECK_GTIDS for current database", flavor)
		}
	}

	// Non-empty results contain binlogs to upload plus one successor needed only for metadata.
	filteredBinlogs := filterBinlogsForUpload(binlogs, cache.LastArchivedBinlog, uploadCtx.UntilBinlog, filter)

	// Nothing to upload
	if len(filteredBinlogs) == 0 {
		return nil
	}

	nextFile, nextGTID, err := openBinlogFile(binlogsFolder, filteredBinlogs[0], flavor, uploadCtx.CheckGTIDs)
	if err != nil {
		return err
	}

	// We expect no gaps between the selected local binlog files, so check only before the first upload.
	if uploadCtx.CheckGTIDs && uploadCtx.TrackGaps && flavor == mysql.MySQLFlavor {
		changed, err := updateSentinelGaps(&binlogSentinelDto, nextGTID, uploadCtx.GTIDPurged, maxBinlogGapEntries)
		tracelog.WarningLogger.PrintOnError(err)
		if changed {
			if err := UploadBinlogSentinel(ctx, rootFolder, &binlogSentinelDto); err != nil {
				return err
			}
		}
	} else {
		binlogSentinelDto.GapTracking = nil
	}

	for i := 0; i < len(filteredBinlogs)-1; i++ {
		currentFile, currentGTID := nextFile, nextGTID
		nextFile, nextGTID, err = openBinlogFile(binlogsFolder, filteredBinlogs[i+1], flavor, uploadCtx.CheckGTIDs)
		if err != nil {
			return err
		}

		if err = uploadBinlog(ctx, uploader, currentFile); err != nil {
			return err
		}
		utility.LoggedClose(currentFile, "failed to close binlog")

		if uploadCtx.CheckGTIDs {
			metadata := &BinlogMetadata{
				BinlogName:        filteredBinlogs[i],
				PreviousGTIDs:     currentGTID,
				NextPreviousGTIDs: nextGTID,
			}
			err = updateSentinelMetadata(&binlogSentinelDto, metadata, flavor)
			tracelog.WarningLogger.PrintOnError(err)

			if err := UploadBinlogSentinel(ctx, uploadCtx.RootFolder, &binlogSentinelDto); err != nil {
				return err
			}
		}

		cache.LastArchivedBinlog = filteredBinlogs[i]
		putCache(cache, uploadCtx.CacheFileName)
	}
	utility.LoggedClose(nextFile, "failed to close binlog")
	return nil
}

// openBinlogFile opens a binlog and reads its previous GTIDs when GTID checking is enabled.
func openBinlogFile(binlogsFolder, binlog, flavor string, checkGTIDs bool) (*os.File, mysql.GTIDSet, error) {
	filename := path.Join(binlogsFolder, binlog)
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open binlog %s: %w", filename, err)
	}
	if !checkGTIDs {
		return file, nil, nil
	}

	previousGTIDs, err := readBinlogFilePreviousGTIDs(file, flavor)
	tracelog.WarningLogger.PrintOnError(err)
	return file, previousGTIDs, nil
}

func updateSentinelMetadata(sentinel *BinlogSentinelDto, metadata *BinlogMetadata, flavor string) error {
	sentinel.LastUploadedBinlog = metadata.BinlogName

	// Header read failures are logged by the caller; preserve the checkpoint and published gaps.
	if metadata.PreviousGTIDs == nil || metadata.NextPreviousGTIDs == nil {
		return nil
	}

	archivedGTIDs, err := mysql.ParseGTIDSet(flavor, sentinel.GTIDArchived)
	if err != nil {
		return fmt.Errorf("failed to apply binlog metadata: %w", err)
	}

	binlogGTIDs := metadata.GTIDs(flavor)
	if err := archivedGTIDs.Update(binlogGTIDs.String()); err != nil {
		return fmt.Errorf("failed to apply binlog metadata: %w", err)
	}

	sentinel.GTIDArchived = archivedGTIDs.String()
	return nil
}

func uploadBinlog(ctx context.Context, uploader internal.Uploader, file *os.File) error {
	tracelog.InfoLogger.Printf("Archiving %s", path.Base(file.Name()))
	if err := uploader.UploadFile(ctx, file); err != nil {
		return fmt.Errorf("failed to upload binlog %s: %w", file.Name(), err)
	}
	return nil
}

// filterBinlogsForUpload returns binlogs to upload plus their successor for metadata.
// It returns an empty slice when no binlogs need uploading.
func filterBinlogsForUpload(binlogs []string, lastArchivedBinlog string, untilBinlog string, filter binlogGtidFilter) []string {
	if len(binlogs) == 0 {
		return binlogs
	}

	// Exclude the active binlog from uploads.
	start, end := 0, len(binlogs)-1

	// If we have local cache - use it to skip all previously uploaded files
	if lastArchivedBinlog != "" {
		for start < end && BinlogNum(binlogs[start]) <= BinlogNum(lastArchivedBinlog) {
			tracelog.DebugLogger.Printf("Skip binlog %v (archived binlog check)\n", binlogs[start])
			start++
		}
	}

	// Apply the exclusive upper bound.
	if untilBinlog != "" {
		for i := start; i < end; i++ {
			if BinlogNum(binlogs[i]) >= BinlogNum(untilBinlog) {
				end = i
				break
			}
		}
	}

	// Skip the initial files whose GTIDs are already archived.
	if filter != nil && filter.isValid() {
		for start < end {
			if filter.shouldUpload(binlogs[start], binlogs[start+1]) {
				break
			}
			tracelog.DebugLogger.Printf("Skip binlog %v (gtid check)\n", binlogs[start])
			start++
		}
	}

	// return empty slice if nothing to upload
	if start == end {
		return []string{}
	}

	// Retain the final file only for reading the preceding file's end-state GTID header.
	return binlogs[start : end+1]
}

func readBinlogSentinel(ctx context.Context, folder storage.Folder) (BinlogSentinelDto, error) {
	var sentinel BinlogSentinelDto
	if err := FetchBinlogSentinel(ctx, folder, &sentinel); err != nil {
		// Use an empty sentinel if none exists.
		var notFound storage.ObjectNotFoundError
		if errors.As(err, &notFound) {
			return BinlogSentinelDto{}, nil
		}
		// Use an empty sentinel if JSON decoding fails.
		var syntaxError *json.SyntaxError
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &syntaxError) || errors.As(err, &typeError) {
			tracelog.WarningLogger.Printf("Failed to parse binlog sentinel; using an empty sentinel: %v", err)
			return BinlogSentinelDto{}, nil
		}
		return BinlogSentinelDto{}, fmt.Errorf("failed to read binlog sentinel: %w", err)
	}

	tracelog.InfoLogger.Printf("Fetched binlog sentinel: %s", &sentinel)
	return sentinel, nil
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

func getCache(cacheFileName string) LogsCache {
	var cache LogsCache
	data, err := os.ReadFile(cacheFileName)
	if err == nil {
		err = json.Unmarshal(data, &cache)
		if err == nil {
			return cache
		}
	}
	if os.IsNotExist(err) {
		tracelog.InfoLogger.Println("MySQL binlog cache does not exist")
	} else {
		tracelog.ErrorLogger.Printf("%+v\n", err)
	}
	return LogsCache{}
}

func putCache(cache LogsCache, cacheFileName string) {
	marshal, err := json.Marshal(&cache)
	if err == nil {
		err = os.WriteFile(cacheFileName, marshal, 0644)
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
