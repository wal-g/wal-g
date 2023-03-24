package postgres

import (
	"fmt"
	"strconv"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

type DatabasesByNames map[string]DatabaseObjectsInfo

// TODO : add tables to info
type DatabaseObjectsInfo struct {
	Oid int `json:"oid"`
}

// TODO : make other query for this job
func (meta DatabasesByNames) appendDatabaseInfos(infos []PgDatabaseInfo) {
	for _, info := range infos {
		meta[info.Name] = DatabaseObjectsInfo{int(info.Oid)}
	}
}

func (meta DatabasesByNames) Resolve(key string) (int, error) {
	if data, ok := meta[key]; ok {
		return data.Oid, nil
	}
	if res, err := strconv.Atoi(key); err == nil {
		return res, nil
	}
	return 0, NewIncorrectNameError(key)
}

type IncorrectNameError struct {
	error
}

func NewIncorrectNameError(name string) IncorrectNameError {
	return IncorrectNameError{errors.Errorf("Can't make directory by oid or find database in meta with name: '%s'", name)}
}

func (err IncorrectNameError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}
