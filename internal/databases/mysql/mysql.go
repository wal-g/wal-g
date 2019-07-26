package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	StreamPrefix          = "stream_"
	BinlogPath            = "binlog_" + utility.VersionStr + "/"
	DatasourceNameSetting = "WALG_MYSQL_DATASOURCE_NAME"
	BinlogEndTsSetting    = "WALG_MYSQL_BINLOG_END_TS"
	BinlogDstSetting      = "WALG_MYSQL_BINLOG_DST"
	BinlogSrcSetting      = "WALG_MYSQL_BINLOG_SRC"
	SslCaSetting          = "WALG_MYSQL_SSL_CA"
)

type Uploader struct {
	*internal.Uploader
}
type Backup struct {
	*internal.Backup
}

func scanToMap(rows *sql.Rows, dst map[string]interface{}) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	args := make([]interface{}, len(columns))
	var garbage interface{}
	for i, field := range columns {
		if v, ok := dst[field]; ok {
			args[i] = v
		} else {
			args[i] = &garbage
		}
	}
	return rows.Scan(args...)
}

// TODO : unit tests
func (backup *Backup) FetchStreamSentinel() (StreamSentinelDto, error) {
	sentinelDto := StreamSentinelDto{}
	sentinelDtoData, err := backup.Backup.FetchSentinelData()
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
	defer utility.LoggedClose(rows, "")
	var logFileName string
	for rows.Next() {
		err = scanToMap(rows, map[string]interface{}{"File": &logFileName})
		if err != nil {
			tracelog.ErrorLogger.Fatalf("%+v\n", err)
		}
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain current binlog file")
	return ""
}

func getMySQLConnection() (*sql.DB, error) {
	if !viper.IsSet(DatasourceNameSetting) {
		return nil, internal.NewUnsetRequiredSettingError(DatasourceNameSetting)
	}
	datasourceName := viper.GetString(DatasourceNameSetting)
	if viper.IsSet(SslCaSetting) {
		caFile := viper.GetString(SslCaSetting)
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
			return nil, fmt.Errorf("MySQL datasource string contains tls option. It can't be used with %v option", SslCaSetting)
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
	dstPath := utility.SanitizePath(path.Join(backup.Name, "stream.")) + extension
	return dstPath
}
