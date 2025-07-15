package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type DatabasesByNames map[string]DatabaseObjectsInfo
type DatabaseObjectsInfo struct {
	Oid    uint32               `json:"oid"`
	Tables map[string]TableInfo `json:"tables_new,omitempty"`
}
type TableInfo struct {
	Oid         uint32               `json:"oid"`
	Relfilenode uint32               `json:"relfilenode"`
	SubTables   map[string]TableInfo `json:"subtables,omitempty"`
}

func NewDatabaseObjectsInfo(oid uint32) *DatabaseObjectsInfo {
	return &DatabaseObjectsInfo{Oid: oid, Tables: make(map[string]TableInfo)}
}

func (meta DatabasesByNames) Resolve(key string) (uint32, uint32, error) {
	database, table, err := meta.unpackKey(key)
	if err != nil {
		return 0, 0, err
	}
	if data, dbFound := meta[database]; dbFound {
		if table == "" {
			return data.Oid, 0, nil
		}
		if tableInfo, tblFound := data.Tables[table]; tblFound {
			return data.Oid, tableInfo.Relfilenode, nil
		}
		return 0, 0, newMetaTableNameError(database, table)
	}
	return 0, 0, newMetaDatabaseNameError(database)
}

func (meta DatabasesByNames) ResolveRegexp(key string) (map[uint32][]uint32, error) {
	database, table, err := meta.unpackKey(key)
	if err != nil {
		return map[uint32][]uint32{}, err
	}
	tracelog.InfoLogger.Printf("unpaсked keys  %s %s", database, table)
	toRestore := map[uint32][]uint32{}
	database = strings.ReplaceAll(database, "*", ".*")
	table = strings.ReplaceAll(table, "*", ".*")
	databaseRegexp := regexp.MustCompile(fmt.Sprintf("^%s$", database))
	tableRegexp := regexp.MustCompile(fmt.Sprintf("^%s$", table))
	for db, dbInfo := range meta {
		if databaseRegexp.MatchString(db) {
			toRestore[dbInfo.Oid] = []uint32{}
			if table == "" {
				tracelog.InfoLogger.Printf("restore all for  %s", db)
			}
			for name, tableInfo := range dbInfo.Tables {
				if table == "" || tableRegexp.MatchString(name) {
					tracelog.InfoLogger.Printf("table to restore through key  %d %s", tableInfo.Relfilenode, table)
					toRestore[dbInfo.Oid] = append(toRestore[dbInfo.Oid], tableInfo.Relfilenode)
					for _, tableInfo2 := range tableInfo.SubTables {
						tracelog.InfoLogger.Printf("subtanble for the table table to restore through key  %d %s", tableInfo2.Relfilenode, table)
						toRestore[dbInfo.Oid] = append(toRestore[dbInfo.Oid], tableInfo2.Relfilenode)
					}
				}
			}
		}
	}
	return toRestore, nil
}

func (meta DatabasesByNames) GetSystemTables() RestoreDesc {
	toRestore := make(RestoreDesc)
	for _, dbInfo := range meta {
		toRestore[dbInfo.Oid] = map[uint32]uint32{}
		for _, tableInfo := range dbInfo.Tables {
			if tableInfo.Oid < systemIDLimit {
				tracelog.DebugLogger.Printf("chose table %d to restore as system one", tableInfo.Oid)
				toRestore[dbInfo.Oid][tableInfo.Relfilenode] = tableInfo.Oid
			}
		}
	}
	return toRestore
}

func (meta DatabasesByNames) tryFormatTableName(table string) (string, bool) {
	tokens := strings.Split(table, ".")
	if len(tokens) == 1 {
		return "public." + tokens[0], true
	} else if len(tokens) == 2 {
		return table, true
	}
	return "", false
}

/*
Unpacks key, which can be:
1. "db" - then we return "db" and empty string for table
2. "db/table" - then we return "db" and "public.table"
3. "db/schema.table" - then we return "db" and "schema.table"
4. "db/schema/table" - then we return "db" and "schema.table"
*/
func (meta DatabasesByNames) unpackKey(key string) (string, string, error) {
	tokens := strings.Split(key, "/")
	switch len(tokens) {
	case 1:
		return tokens[0], "", nil
	case 2:
		table, ok := meta.tryFormatTableName(tokens[1])
		if !ok {
			return "", "", newMetaIncorrectKeyError(key)
		}
		return tokens[0], table, nil
	case 3:
		table, ok := meta.tryFormatTableName(fmt.Sprintf("%s.%s", tokens[1], tokens[2]))
		if !ok {
			return "", "", newMetaIncorrectKeyError(key)
		}
		return tokens[0], table, nil
	default:
		return "", "", newMetaIncorrectKeyError(key)
	}
}

type metaDatabaseNameError struct {
	error
}

func newMetaDatabaseNameError(databaseName string) metaDatabaseNameError {
	return metaDatabaseNameError{errors.Errorf("Can't find database in meta with name: '%s'", databaseName)}
}
func (err metaDatabaseNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type metaTableNameError struct {
	error
}

func newMetaTableNameError(databaseName, tableName string) metaTableNameError {
	return metaTableNameError{
		errors.Errorf("Can't find table in meta for '%s' database and name: '%s'", databaseName, tableName)}
}
func (err metaTableNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type metaIncorrectKeyError struct {
	error
}

func newMetaIncorrectKeyError(key string) metaIncorrectKeyError {
	return metaIncorrectKeyError{
		errors.Errorf("Unexpected format of database or table to restore: '%s'. "+
			"Use 'dat', 'dat/rel' or 'dat/nmsp.rel'", key)}
}
func (err metaIncorrectKeyError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
