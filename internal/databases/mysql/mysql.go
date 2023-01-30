package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/wal-g/internal/compression"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const BinlogPath = "binlog_" + utility.VersionStr + "/"

const TimeMysqlFormat = "2006-01-02 15:04:05"

func getMySQLFlavor(db *sql.DB) (string, error) {
	row := db.QueryRow("SELECT @@version")
	var versionComment string
	err := row.Scan(&versionComment)
	if err != nil {
		return "", err
	}
	// example: '10.6.4-MariaDB-1:10.6.4+maria~focal'
	if strings.Contains(versionComment, "MariaDB") {
		return gomysql.MariaDBFlavor, nil
	}
	// It is possible to distinguish Percona & MySQL by checking 'version_comment',
	// however usually we can expect that there is no difference between these distributions
	return gomysql.MySQLFlavor, nil
}

func getMySQLGTIDExecuted(db *sql.DB, flavor string) (gomysql.GTIDSet, error) {
	query := ""
	switch flavor {
	case gomysql.MySQLFlavor:
		query = "SELECT @@global.gtid_executed"
	case gomysql.MariaDBFlavor:
		query = "SELECT @@global.gtid_current_pos"
	default:
		return nil, fmt.Errorf("unknown MySQL flavor: %s", flavor)
	}

	gtidStr := ""
	row := db.QueryRow(query)
	err := row.Scan(&gtidStr)
	if err != nil {
		return nil, err
	}

	return gomysql.ParseGTIDSet(flavor, gtidStr)
}

func getLastUploadedBinlog(folder storage.Folder) (string, error) {
	logFiles, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return "", err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})
	if len(logFiles) == 0 {
		return "", nil
	}
	name := logFiles[len(logFiles)-1].GetName()
	if ext := path.Ext(name); compression.FindDecompressor(ext) != nil {
		// remove archive extension (like .br)
		name = strings.TrimSuffix(name, ext)
	}
	return name, nil
}

func getLastUploadedBinlogBeforeGTID(folder storage.Folder, gtid gomysql.GTIDSet, flavor string) (string, error) {
	folder = folder.GetSubFolder(BinlogPath)
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return "", err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})
	if len(logFiles) == 0 {
		return "", nil
	}
	for i := len(logFiles) - 1; i > 0; i-- {
		prevGtid, err := GetBinlogPreviousGTIDsRemote(folder, logFiles[i].GetName(), flavor)
		if err != nil {
			return "", err
		}
		if gtid.Contain(prevGtid) {
			return utility.TrimFileExtension(logFiles[i].GetName()), nil
		}
	}
	tracelog.WarningLogger.Printf("failed to find uploaded binlog behind %s", gtid)
	return "", nil
}

func getPositionBeforeGTID(folder storage.Folder, gtidSet gomysql.GTIDSet, flavor string) (gomysql.Position, error) {
	var pos gomysql.Position
	var err error
	pos.Name, err = getLastUploadedBinlogBeforeGTID(folder, gtidSet, flavor)
	if err != nil {
		return gomysql.Position{}, err
	}
	pos.Pos = 4
	return pos, err
}

func getMySQLConnection() (*sql.DB, error) {
	datasourceName, err := internal.GetRequiredSetting(internal.MysqlDatasourceNameSetting)
	if err != nil {
		return nil, err
	}
	db, err := getMySQLConnectionFromDatasource(datasourceName)
	if err != nil {
		fallbackDatasourceName := replaceHostInDatasourceName(datasourceName, "localhost")
		if fallbackDatasourceName != datasourceName {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided host, trying localhost")

			db, err = getMySQLConnectionFromDatasource(datasourceName)
		}
	}
	return db, err
}

func getMySQLConnectionFromDatasource(datasourceName string) (*sql.DB, error) {
	if caFile, ok := internal.GetSetting(internal.MysqlSslCaSetting); ok {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("failed to load certificate from %s", caFile)
		}
		err = mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		})
		if err != nil {
			return nil, err
		}
		if strings.Contains(datasourceName, "?tls=") || strings.Contains(datasourceName, "&tls=") {
			return nil,
				fmt.Errorf("mySQL datasource string contains tls option. It can't be used with %v option",
					internal.MysqlSslCaSetting)
		}
		if strings.Contains(datasourceName, "?") {
			datasourceName += "&tls=custom"
		} else {
			datasourceName += "?tls=custom"
		}
	}
	db, err := sql.Open("mysql", datasourceName)
	return db, err
}

func replaceHostInDatasourceName(datasourceName string, newHost string) string {
	var userData, dbNameAndParams string

	splitName := strings.SplitN(datasourceName, "@", 2)
	if len(splitName) == 2 {
		userData = splitName[0]
	} else {
		userData = ""
	}
	splitName = strings.SplitN(datasourceName, "/", 2)
	if len(splitName) == 2 {
		dbNameAndParams = splitName[1]
	} else {
		dbNameAndParams = ""
	}

	return userData + "@" + newHost + "/" + dbNameAndParams
}

type StreamSentinelDto struct {
	BinLogStart string `json:"BinLogStart,omitempty"`
	// BinLogEnd field is for debug purpose only.
	// As we can not guarantee that transactions in BinLogEnd file happened before or after backup
	BinLogEnd      string    `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time `json:"StartLocalTime,omitempty"`
	StopLocalTime  time.Time `json:"StopLocalTime,omitempty"`

	UncompressedSize int64  `json:"UncompressedSize,omitempty"`
	CompressedSize   int64  `json:"CompressedSize,omitempty"`
	Hostname         string `json:"Hostname,omitempty"`

	IsPermanent bool        `json:"IsPermanent,omitempty"`
	UserData    interface{} `json:"UserData,omitempty"`

	//todo: add other fields from internal.GenericMetadata
}

func (s *StreamSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

type binlogHandler interface {
	handleBinlog(binlogPath string) error
}

func fetchLogs(folder storage.Folder, dstDir string, startTS, endTS, endBinlogTS time.Time, handler binlogHandler) error {
	logFolder := folder.GetSubFolder(BinlogPath)
	includeStart := true
outer:
	for {
		logsToFetch, err := getLogsCoveringInterval(logFolder, startTS, includeStart, endBinlogTS)
		includeStart = false
		if err != nil {
			return err
		}
		for _, logFile := range logsToFetch {
			startTS = logFile.GetLastModified()
			binlogName := utility.TrimFileExtension(logFile.GetName())
			binlogPath := path.Join(dstDir, binlogName)
			tracelog.InfoLogger.Printf("downloading %s into %s", binlogName, binlogPath)
			if err = internal.DownloadFileTo(logFolder, binlogName, binlogPath); err != nil {
				tracelog.ErrorLogger.Printf("failed to download %s: %v", binlogName, err)
				return err
			}
			timestamp, err := GetBinlogStartTimestamp(binlogPath, gomysql.MySQLFlavor)
			if err != nil {
				return err
			}
			err = handler.handleBinlog(binlogPath)
			if err != nil {
				return err
			}
			if timestamp.After(endTS) {
				break outer
			}
		}
		if len(logsToFetch) == 0 {
			break
		}
	}
	return nil
}

func handleObjectProviderError(err error, p *storage.ObjectProvider) {
	if err == nil {
		return
	}
	ok := p.AddErrorToProvider(err)
	for !ok {
		ok = p.AddErrorToProvider(err)
	}
}

func provideLogs(folder storage.Folder, dstDir string, startTS, endTS time.Time, p *storage.ObjectProvider) {
	defer p.Close()
	_, err := os.Stat(dstDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(dstDir, 0777)
		handleObjectProviderError(err, p)
		if err != nil {
			return
		}
	}

	logFolder := folder.GetSubFolder(BinlogPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTS, true, utility.MaxTime)
	handleObjectProviderError(err, p)
	if err != nil {
		return
	}

	for _, logFile := range logsToFetch {
		// download log files
		binlogName := utility.TrimFileExtension(logFile.GetName())
		binlogPath := path.Join(dstDir, binlogName)
		tracelog.InfoLogger.Printf("downloading %s into %s", binlogName, binlogPath)
		if err = internal.DownloadFileTo(logFolder, binlogName, binlogPath); err != nil {
			if os.IsExist(err) {
				tracelog.WarningLogger.Printf("file %s exist skipping", binlogName)
			} else {
				tracelog.ErrorLogger.Printf("failed to download %s: %v", binlogName, err)
				handleObjectProviderError(err, p)
				return
			}
		}

		// add file to provider
		err = p.AddObjectToProvider(logFile)
		handleObjectProviderError(err, p)
		if err != nil {
			return
		}

		timestamp, err := GetBinlogStartTimestamp(binlogPath, gomysql.MySQLFlavor)
		handleObjectProviderError(err, p)
		if err != nil {
			return
		}
		if timestamp.After(endTS) {
			return
		}
	}
}

func getBinlogSinceTS(folder storage.Folder, backup internal.Backup) (time.Time, error) {
	startTS := utility.MaxTime // far future
	var streamSentinel StreamSentinelDto
	err := backup.FetchSentinel(&streamSentinel)
	if err != nil {
		return time.Time{}, err
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", streamSentinel.String())

	// case when backup was uploaded before first binlog
	sentinels, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, sentinel := range sentinels {
		if strings.HasPrefix(sentinel.GetName(), backup.Name) {
			tracelog.InfoLogger.Printf("Backup sentinel file: %s (%s)", sentinel.GetName(), sentinel.GetLastModified())
			if sentinel.GetLastModified().Before(startTS) {
				startTS = sentinel.GetLastModified()
			}
		}
	}
	// case when binlog was uploaded before backup
	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			tracelog.InfoLogger.Printf("Backup start binlog: %s (%s)", binlog.GetName(), binlog.GetLastModified())
			if binlog.GetLastModified().Before(startTS) {
				startTS = binlog.GetLastModified()
			}
		}
	}
	return startTS, nil
}

// getLogsCoveringInterval lists the operation logs that cover the interval
func getLogsCoveringInterval(folder storage.Folder, start time.Time, includeStart bool, endBinlogTS time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})
	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		if logFile.GetLastModified().After(endBinlogTS) {
			continue // don't fetch binlogs from future
		}
		if start.Before(logFile.GetLastModified()) || includeStart && start.Equal(logFile.GetLastModified()) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}
