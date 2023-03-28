package postgres

import (
	"github.com/wal-g/tracelog"
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/wal-g/internal"
)

const (
	defaultTbspPrefix = "/" + DefaultTablespace + "/"
	customTbspPrefix  = "/" + TablespaceFolder + "/"
	systemIDLimit     = 16384
)

type ExtractProviderDBSpec struct {
	ExtractProviderImpl
	onlyDatabases []string
}

func NewExtractProviderDBSpec(onlyDatabases []string) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{onlyDatabases: onlyDatabases}
}

func (p ExtractProviderDBSpec) Get(
	backup Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (IncrementalTarInterpreter, []internal.ReaderMaker, string, error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	tracelog.ErrorLogger.FatalOnError(err)

	fullRestoreDatabases := p.makeFullRestoreDatabaseMap(p.onlyDatabases, filesMeta.DatabasesByNames)
	p.filterFilesToUnwrap(filesToUnwrap, fullRestoreDatabases)

	return p.ExtractProviderImpl.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}

func (p ExtractProviderDBSpec) makeFullRestoreDatabaseMap(databases []string, names DatabasesByNames) map[int]bool {
	restoredDatabases := p.makeSystemDatabasesMap()

	for _, db := range databases {
		dbID, err := names.Resolve(db)
		if err == nil {
			restoredDatabases[dbID] = true
		}
	}

	return restoredDatabases
}

func (p ExtractProviderDBSpec) makeSystemDatabasesMap() map[int]bool {
	restoredDatabases := make(map[int]bool)
	for i := 1; i < systemIDLimit; i++ {
		restoredDatabases[i] = true
	}
	return restoredDatabases
}

func (p ExtractProviderDBSpec) filterFilesToUnwrap(filesToUnwrap map[string]bool, databases map[int]bool) {
	for file := range filesToUnwrap {
		isDB, dbID, _ := p.tryGetOidPair(file)

		if isDB && !databases[dbID] {
			delete(filesToUnwrap, file)
		}
	}
}

func (p ExtractProviderDBSpec) tryGetOidPair(file string) (bool, int, int) {
	if !(strings.HasPrefix(file, defaultTbspPrefix) || strings.HasPrefix(file, customTbspPrefix)) {
		return false, 0, 0
	}
	var tableID, dbID int

	file, tableID = p.cutIntegerBase(file)
	_, dbID = p.cutIntegerBase(file)

	return true, dbID, tableID
}

func (p ExtractProviderDBSpec) cutIntegerBase(file string) (string, int) {
	parent, base := path.Dir(file), path.Base(file)
	base, _, _ = strings.Cut(base, ".")
	base, _, _ = strings.Cut(base, "_")
	integerResult, _ := strconv.Atoi(base)
	return parent, integerResult
}
