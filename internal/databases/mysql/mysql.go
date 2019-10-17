package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/go-sql-driver/mysql"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
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

func getMySQLCurrentBinlogFile(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW MASTER STATUS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	var logFileName string
	for rows.Next() {
		err = scanToMap(rows, map[string]interface{}{"File": &logFileName})
		tracelog.ErrorLogger.FatalOnError(err)
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
	db, err := getMySqlConnectionFromDatasource(datasourceName)
	if err != nil {
		fallbackDatasourceName := replaceHostInDatasourceName(datasourceName, "localhost")
		if fallbackDatasourceName != datasourceName {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided host, trying localhost")

			db, err = getMySqlConnectionFromDatasource(datasourceName)
		}
	}
	return db, err
}

func getMySqlConnectionFromDatasource(datasourceName string) (*sql.DB, error) {
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
		err = mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		})
		if err != nil {
			return nil, err
		}
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
	BinLogStart    string `json:"BinLogStart,omitempty"`
	BinLogEnd      string `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time
}
