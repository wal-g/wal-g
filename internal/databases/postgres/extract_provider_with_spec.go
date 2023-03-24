package postgres

import (
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/wal-g/internal"
)

const (
	defaultTbspPrefix = "/" + DefaultTablespace + "/"
	customTbspPrefix  = "/" + TablespaceFolder + "/"
	systemIdLimit     = 16384
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
	_, filesMeta, _ := backup.GetSentinelAndFilesMetadata()

	fullRestoreDatabases := p.makeFullRestoreDatabaseMap(p.onlyDatabases, filesMeta.DatabasesByNames)
	p.filterFilesToUnwrap(filesToUnwrap, fullRestoreDatabases)

	return p.ExtractProviderImpl.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}

func (p ExtractProviderDBSpec) makeFullRestoreDatabaseMap(databases []string, names DatabasesByNames) map[int]bool {
	restoredDatabases := p.makeSystemDatabasesMap()

	for _, db := range databases {
		dbId, err := names.Resolve(db)
		if err == nil {
			restoredDatabases[dbId] = true
		}
	}

	return restoredDatabases
}

func (p ExtractProviderDBSpec) makeSystemDatabasesMap() map[int]bool {
	restoredDatabases := make(map[int]bool)
	for i := 1; i < systemIdLimit; i++ {
		restoredDatabases[i] = true
	}
	return restoredDatabases
}

func (p ExtractProviderDBSpec) filterFilesToUnwrap(filesToUnwrap map[string]bool, databases map[int]bool) {
	for file := range filesToUnwrap {
		isDb, dbId, _ := p.tryGetOidPair(file)

		if isDb && !databases[dbId] {
			delete(filesToUnwrap, file)
		}
	}
}

func (p ExtractProviderDBSpec) tryGetOidPair(file string) (bool, int, int) {
	if !(strings.HasPrefix(file, defaultTbspPrefix) || strings.HasPrefix(file, customTbspPrefix)) {
		return false, 0, 0
	}
	var tableId, dbId int

	file, tableId = p.cutIntegerBase(file)
	file, dbId = p.cutIntegerBase(file)

	return true, dbId, tableId
}

func (p ExtractProviderDBSpec) cutIntegerBase(file string) (string, int) {
	parent, base := path.Dir(file), path.Base(file)
	base, _, _ = strings.Cut(base, ".")
	base, _, _ = strings.Cut(base, "_")
	integerResult, _ := strconv.Atoi(base)
	return parent, integerResult
}
