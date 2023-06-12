package postgres

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type DatabasesByNames map[string]DatabaseObjectsInfo

type DatabaseObjectsInfo struct {
	Oid    uint32            `json:"oid"`
	Tables map[string]uint32 `json:"tables,omitempty"`
}

func NewDatabaseObjectsInfo(oid uint32) *DatabaseObjectsInfo {
	return &DatabaseObjectsInfo{Oid: oid, Tables: make(map[string]uint32)}
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
		if tableFile, tblFound := data.Tables[table]; tblFound {
			return data.Oid, tableFile, nil
		}
		return 0, 0, newMetaTableNameError(database, table)
	}
	return 0, 0, newMetaDatabaseNameError(database)
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

func (meta DatabasesByNames) unpackKey(key string) (string, string, error) {
	tokens := strings.Split(key, "/")
	if len(tokens) < 2 {
		return tokens[0], "", nil
	}
	if len(tokens) > 2 {
		return "", "", newMetaIncorrectKeyError(key)
	}

	table, ok := meta.tryFormatTableName(tokens[1])
	if !ok {
		return "", "", newMetaIncorrectKeyError(key)
	}

	return tokens[0], table, nil
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
