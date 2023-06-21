package greenplum

import (
	"strings"

	"github.com/wal-g/tracelog"
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
	restoreParameters []string
}

func NewExtractProviderDBSpec(restoreParameters []string) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{restoreParameters}
}

func (p ExtractProviderDBSpec) Get(
	backup postgres.Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (postgres.IncrementalTarInterpreter, []internal.ReaderMaker, string, error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	tracelog.ErrorLogger.FatalOnError(err)

	desc, err := RestoreDescMaker{}.Make(p.restoreParameters, filesMeta.DatabasesByNames)
	tracelog.ErrorLogger.FatalOnError(err)
	desc.FilterFilesToUnwrap(filesToUnwrap)

	return ExtractProviderImpl{}.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}
