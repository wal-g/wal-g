package sqlserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/sqlserver/blob"
	"github.com/wal-g/wal-g/utility"
)

const AllDatabases = "ALL"

const LogNamePrefix = "wal_"

const TimeSQLServerFormat = "Jan 02, 2006 03:04 PM"

const MaxTransferSize = 4 * 1024 * 1024

const MaxBlocksPerBlob = 25000 // 50000 actually, but we need some safety margin

const MaxBlobSize = MaxTransferSize * MaxBlocksPerBlob

const BlobNamePrefix = "blob_"

var SystemDbnames = []string{
	"master",
	"msdb",
	"model",
}

type SentinelDto struct {
	Server         string
	Databases      []string
	StartLocalTime time.Time
}

func (s *SentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func getSQLServerConnection() (*sql.DB, error) {
	connString, err := internal.GetRequiredSetting(internal.SQLServerConnectionString)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func getDatabasesToBackup(db *sql.DB, dbnames []string) ([]string, error) {
	allDbnames, err := listDatabases(db)
	if err != nil {
		return nil, err
	}
	switch {
	case len(dbnames) == 1 && dbnames[0] == AllDatabases:
		return allDbnames, nil
	case len(dbnames) > 0:
		missing := exclude(dbnames, allDbnames)
		if len(missing) > 0 {
			return nil, fmt.Errorf("databases %v were not found in server", missing)
		}
		return dbnames, nil
	default:
		return exclude(allDbnames, SystemDbnames), nil
	}
}

func getDatabasesToRestore(sentinel *SentinelDto, dbnames []string) ([]string, error) {
	switch {
	case len(dbnames) == 1 && dbnames[0] == AllDatabases:
		return sentinel.Databases, nil
	case len(dbnames) > 0:
		missing := exclude(dbnames, sentinel.Databases)
		if len(missing) > 0 {
			return nil, fmt.Errorf("databases %v were not found in backup", missing)
		}
		return dbnames, nil
	default:
		return exclude(sentinel.Databases, SystemDbnames), nil
	}
}

func listDatabases(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT name FROM SYS.DATABASES WHERE name != 'tempdb'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func estimateSize(db *sql.DB, query string, args ...interface{}) (int64, int, error) {
	var size int64
	row := db.QueryRow(query, args...)
	err := row.Scan(&size)
	if err != nil {
		return 0, 0, err
	}
	blobCount := int(math.Ceil(float64(size) / float64(MaxBlobSize)))
	return size, blobCount, nil
}

func estimateDbSize(db *sql.DB, dbname string) (int64, int, error) {
	query := fmt.Sprintf(`
		USE %s; 
		SELECT (SELECT used_log_space_in_bytes FROM sys.dm_db_log_space_usage) 
			 + (SELECT allocated_extent_page_count*8*1024 FROM sys.dm_db_file_space_usage)
		USE master;
	`, quoteName(dbname))
	return estimateSize(db, query)
}

func estimateLogSize(db *sql.DB, dbname string) (int64, int, error) {
	query := fmt.Sprintf(`
		USE %s; 
		SELECT log_space_in_bytes_since_last_backup FROM sys.dm_db_log_space_usage; 
		USE master;
	`, quoteName(dbname))
	return estimateSize(db, query)
}

func buildBackupUrls(baseUrl string, blobCount int) string {
	res := ""
	for i := 0; i < blobCount; i++ {
		if i > 0 {
			res += ", "
		}
		blobName := fmt.Sprintf("%s%03d", BlobNamePrefix, i)
		res += fmt.Sprintf("URL = '%s/%s'", baseUrl, blobName)
	}
	return res
}

func buildRestoreUrls(baseUrl string, blobNames []string) string {
	if len(blobNames) == 0 {
		// old-style single blob backup
		return fmt.Sprintf("URL = '%s'", baseUrl)
	}
	res := ""
	for i, blobName := range blobNames {
		if i > 0 {
			res += ", "
		}
		res += fmt.Sprintf("URL = '%s/%s'", baseUrl, blobName)
	}
	return res
}

func quoteName(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func generateDatabaseBackupName() string {
	return utility.BackupNamePrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func getDatabaseBackupPath(backupName, dbname string) string {
	return path.Join(utility.BaseBackupPath, backupName, dbname)
}

func getDatabaseBackupUrl(backupName, dbname string) string {
	hostname, err := internal.GetRequiredSetting(internal.SQLServerBlobHostname)
	if err != nil {
		tracelog.ErrorLogger.FatalOnError(err)
	}
	backupName = url.QueryEscape(backupName)
	dbname = url.QueryEscape(dbname)
	return fmt.Sprintf("https://%s/%s", hostname, getDatabaseBackupPath(backupName, dbname))
}

func generateLogBackupName() string {
	return LogNamePrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func getLogBackupPath(logBackupName, dbname string) string {
	return path.Join(utility.WalPath, logBackupName, dbname)
}

func getLogBackupUrl(logBackupName, dbname string) string {
	hostname, err := internal.GetRequiredSetting(internal.SQLServerBlobHostname)
	if err != nil {
		tracelog.ErrorLogger.FatalOnError(err)
	}
	logBackupName = url.QueryEscape(logBackupName)
	dbname = url.QueryEscape(dbname)
	return fmt.Sprintf("https://%s/%s", hostname, getLogBackupPath(logBackupName, dbname))
}

func doesLogBackupContainDb(folder storage.Folder, logBakupName string, dbname string) (bool, error) {
	f := folder.GetSubFolder(utility.WalPath).GetSubFolder(logBakupName)
	_, dbDirs, err := f.ListFolder()
	if err != nil {
		return false, err
	}
	for _, dbDir := range dbDirs {
		if dbname == path.Base(dbDir.GetPath()) {
			return true, nil
		}
	}
	return false, nil
}

func listBackupBlobs(folder storage.Folder) ([]string, error) {
	ok, err := folder.Exists(blob.IndexFileName)
	if err != nil {
		return nil, err
	}
	if ok {
		// old-style single blob backup
		return nil, nil
	}
	_, blobDirs, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	var blobs []string
	for _, blobDir := range blobDirs {
		name := path.Base(blobDir.GetPath())
		if strings.HasPrefix(name, BlobNamePrefix) {
			blobs = append(blobs, name)
		}
	}
	sort.Strings(blobs)
	return blobs, nil
}

func getLogsSinceBackup(folder storage.Folder, backupName string, stopAt time.Time) ([]string, error) {
	if !strings.HasPrefix(backupName, utility.BackupNamePrefix) {
		return nil, fmt.Errorf("unexpected backup name: %s", backupName)
	}
	startTs := backupName[len(utility.BackupNamePrefix):]
	endTs := stopAt.Format(utility.BackupTimeFormat)
	_, logBackups, err := folder.GetSubFolder(utility.WalPath).ListFolder()
	if err != nil {
		return nil, err
	}
	var allLogNames []string
	for _, logBackup := range logBackups {
		allLogNames = append(allLogNames, path.Base(logBackup.GetPath()))
	}
	sort.Strings(allLogNames)

	var logNames []string
	for _, name := range allLogNames {
		logTs := name[len(LogNamePrefix):]
		if logTs < startTs {
			continue
		}
		logNames = append(logNames, name)
		if logTs > endTs {
			break
		}
	}

	return logNames, nil
}

func runParallel(f func(string) error, dbnames []string) error {
	errs := make(chan error, len(dbnames))
	for _, dbname := range dbnames {
		go func(dbname string) {
			errs <- f(dbname)
		}(dbname)
	}
	var errStr string
	for i := 0; i < len(dbnames); i++ {
		err := <-errs
		if err != nil {
			errStr += err.Error() + "\n"
		}
	}
	if errStr != "" {
		return errors.New(errStr)
	}
	return nil
}

func exclude(src, excl []string) []string {
	var res []string
SRC:
	for _, r := range src {
		for _, r2 := range excl {
			if r2 == r {
				continue SRC
			}
		}
		res = append(res, r)
	}
	return res
}

func uniq(src []string) []string {
	res := make([]string, 0, len(src))
	done := make(map[string]struct{}, len(src))
	for _, s := range src {
		if _, ok := done[s]; !ok {
			res = append(res, s)
			done[s] = struct{}{}
		}
	}
	return res
}
