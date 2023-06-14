package greenplum

import (
	"strings"

	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const (
	aoSegNamespace = "pg_aoseg."
)

type RestoreDescMaker struct{}

func (m RestoreDescMaker) Make(restoreParameters []string, names postgres.DatabasesByNames) (postgres.RestoreDesc, error) {
	restoredDatabases, err := postgres.DefaultRestoreDescMaker{}.Make(restoreParameters, names)
	if err != nil {
		return nil, err
	}

	for _, dbInfo := range names {
		for table, tableID := range dbInfo.Tables {
			if m.FromAoSegNamespace(table) {
				restoredDatabases.Add(dbInfo.Oid, tableID)
			}
		}
	}

	return restoredDatabases, nil
}

func (m RestoreDescMaker) FromAoSegNamespace(tableName string) bool {
	return strings.HasPrefix(tableName, aoSegNamespace)
}

func NewExtractProviderDBSpec(restoreParameters []string) *postgres.ExtractProviderDBSpec {
	extractor := postgres.NewExtractProviderDBSpec(restoreParameters)
	extractor.ExtractProviderImpl = ExtractProviderImpl{}
	extractor.RestoreDescMaker = RestoreDescMaker{}
	return extractor
}
