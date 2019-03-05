package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io/ioutil"
	"path"
	"strings"
	"time"
)

const (
	BinlogPath               = "binlog_" + internal.VersionStr + "/"
	StreamPrefix             = "stream_"
)

type Uploader struct {
	*internal.Uploader
}
type Backup struct {
	*internal.Backup
}

func DeleteOldBinlogs(backupTime internal.BackupTime, folder storage.Folder) {
	if !strings.HasPrefix(backupTime.BackupName, StreamPrefix) {
		return
	}
	backup := Backup{internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), backupTime.BackupName)}
	dto, err := backup.fetchStreamSentinel()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	binlogSkip := dto.BinLogStart
	tracelog.InfoLogger.Println("Delete binlog before", binlogSkip)
	internal.DeleteWALBefore(binlogSkip, folder.GetSubFolder(BinlogPath))
}

// TODO : unit tests
func (backup *Backup) fetchStreamSentinel() (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	backupReaderMaker := internal.NewStorageReaderMaker(backup.BaseBackupFolder,
		backup.GetStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return sentinelDto, err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDto, errors.Wrap(err, "failed to fetch sentinel")
	}

	err = json.Unmarshal(sentinelDtoData, &sentinelDto)
	return sentinelDto, errors.Wrap(err, "failed to unmarshal sentinel")
}

func getMySQLCurrentBinlogFile(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	var logFileName string
	var garbage interface{}
	for rows.Next() {
		err = rows.Scan(&logFileName, &garbage, &garbage, &garbage, &garbage)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		return logFileName
	}
	rows, err = db.Query("SHOW SLAVE STATUS")
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}
	for rows.Next() {
		err = rows.Scan(&logFileName, &garbage, &garbage, &garbage, &garbage)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain current binlog file")
	return ""
}

func getMySQLConnection() (*sql.DB, error) {
	datasourceName := internal.GetSettingValue("WALG_MYSQL_DATASOURCE_NAME")
	if datasourceName == "" {
		datasourceName = "root:password@/mysql"
	}
	caFile := internal.GetSettingValue("WALG_MYSQL_SSL_CA")
	if caFile != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("Failed to load certificate from %s", caFile)
		}
		mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		})
		if strings.Contains(datasourceName, "?tls=") || strings.Contains(datasourceName, "&tls=") {
			return nil, fmt.Errorf("MySQL datasource string contains tls option. It can't be used with WALG_MYSQL_SSL_CA option")
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

type StreamSentinelDto struct {
	BinLogStart    string `json:"BinLogStart,omitempty"`
	BinLogEnd      string `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time
}

func getStreamName(backup *Backup, extension string) string {
	dstPath := internal.SanitizePath(path.Join(backup.Name, "stream.")) + extension
	return dstPath
}

func loggedClose(obj interface{Close() error}) {
	if err := obj.Close(); err != nil {
		tracelog.ErrorLogger.Fatal(err)
	}
}