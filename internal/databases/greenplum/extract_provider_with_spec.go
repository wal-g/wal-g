package greenplum

import (
	"strings"

	"github.com/wal-g/wal-g/internal"
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
		if _, dbNotSkipped := restoredDatabases[dbInfo.Oid]; !dbNotSkipped {
			continue
		}
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

type ExtractProviderDBSpec struct {
	*postgres.ExtractProviderDBSpec
}

func NewExtractProviderDBSpec(restoreParameters []string) *ExtractProviderDBSpec {
	extractor := postgres.NewExtractProviderDBSpec(restoreParameters)
	extractor.ExtractProviderImpl = ExtractProviderImpl{}
	extractor.RestoreDescMaker = RestoreDescMaker{}
	return &ExtractProviderDBSpec{ExtractProviderDBSpec: extractor}
}

func (p ExtractProviderDBSpec) Get(
	backup postgres.Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (postgres.IncrementalTarInterpreter, []internal.ReaderMaker, string, error) {
	return p.ExtractProviderDBSpec.Get(backup, filesToUnwrap, true, dbDataDir, createNewIncrementalFiles)
}
