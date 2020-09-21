package sqlserver

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const AllDatabases = "ALL"

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
	rows, err := db.Query("SELECT NAME FROM SYS.DATABASES WHERE NAME != 'tempdb'")
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

func quoteName(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func generateBackupName() string {
	return utility.BackupNamePrefix + utility.TimeNowCrossPlatformUTC().Format(utility.BackupTimeFormat)
}

func getBackupUrl(backupName string) string {
	hostname, err := internal.GetRequiredSetting(internal.SQLServerBlobHostname)
	if err != nil {
		tracelog.ErrorLogger.FatalOnError(err)
	}
	return fmt.Sprintf("https://%s/%s/%s", hostname, utility.BaseBackupPath, backupName)
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
