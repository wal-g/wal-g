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
	restoredDatabases, err := postgres.RegexpRestoreDescMaker{}.Make(restoreParameters, names)
	if err != nil {
		return nil, err
	}

	for _, dbInfo := range names {
		for table, tableInfo := range dbInfo.Tables {
			if m.FromAoSegNamespace(table) {
				restoredDatabases.Add(dbInfo.Oid, tableInfo.Relfilenode, tableInfo.Oid)
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
	restoreDescMaker  RestoreDescMaker
}

func NewExtractProviderDBSpec(restoreParameters []string) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{restoreParameters, RestoreDescMaker{}}
}

func (p ExtractProviderDBSpec) Get(
	backup postgres.Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (postgres.IncrementalTarInterpreter, []internal.ReaderMaker, []internal.ReaderMaker, error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	tracelog.ErrorLogger.FatalOnError(err)

	desc, err := p.restoreDescMaker.Make(p.restoreParameters, filesMeta.DatabasesByNames)
	tracelog.ErrorLogger.FatalOnError(err)
	desc.FilterFilesToUnwrap(filesToUnwrap)

	return ExtractProviderImpl{}.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}
