package postgres

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type DatabasesByNames map[string]DatabaseObjectsInfo

type DatabaseObjectsInfo struct {
	Oid    int               `json:"oid"`
	Tables map[string]uint32 `json:"tables,omitempty"`
}

func NewDatabaseObjectsInfo(oid int) *DatabaseObjectsInfo {
	return &DatabaseObjectsInfo{Oid: oid, Tables: make(map[string]uint32)}
}

func (meta DatabasesByNames) Resolve(key string) (int, error) {
	if data, ok := meta[key]; ok {
		return data.Oid, nil
	}
	return 0, NewIncorrectNameError(key)
}

type IncorrectNameError struct {
	error
}

func NewIncorrectNameError(name string) IncorrectNameError {
	return IncorrectNameError{errors.Errorf("Can't find database in meta with name: '%s'", name)}
}

func (err IncorrectNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
