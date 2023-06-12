package postgres

import (
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	defaultTbspPrefix = "/" + DefaultTablespace + "/"
	customTbspPrefix  = "/" + TablespaceFolder + "/"
	systemIDLimit     = 16384
)

type RestoreDesc map[uint32]map[uint32]bool

func (desc RestoreDesc) Add(database, table uint32) {
	if _, ok := desc[database]; !ok {
		desc[database] = make(map[uint32]bool)
	}
	desc[database][table] = true
}

func (desc RestoreDesc) IsFull(database uint32) bool {
	if _, ok := desc[database]; ok {
		return desc[database][0]
	}
	return false
}

func (desc RestoreDesc) IsSkipped(database, table uint32) bool {
	if database < systemIDLimit || desc.IsFull(database) {
		return false
	}
	if _, ok := desc[database]; ok {
		_, found := desc[database][table]
		return table >= systemIDLimit && !found
	}
	return true
}

type ExtractProviderDBSpec struct {
	ExtractProviderImpl ExtractProvider
	restoreParameters   []string
}

func NewExtractProviderDBSpec(partialRestoreParameters []string) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{ExtractProviderImpl: ExtractProviderImpl{}, restoreParameters: partialRestoreParameters}
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

	desc, err := p.makeRestoreDesc(p.restoreParameters, filesMeta.DatabasesByNames)
	tracelog.ErrorLogger.FatalOnError(err)
	p.filterFilesToUnwrap(filesToUnwrap, desc)

	return p.ExtractProviderImpl.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}

func (p ExtractProviderDBSpec) makeRestoreDesc(restoreParameters []string, names DatabasesByNames) (RestoreDesc, error) {
	restoredDatabases := make(RestoreDesc)

	for _, parameter := range restoreParameters {
		dbID, tableID, err := names.Resolve(parameter)
		if err != nil {
			return nil, err
		}

		restoredDatabases.Add(dbID, tableID)
	}

	return restoredDatabases, nil
}

func (p ExtractProviderDBSpec) filterFilesToUnwrap(filesToUnwrap map[string]bool, desc RestoreDesc) {
	for file := range filesToUnwrap {
		isDB, dbID, tableID := p.TryGetOidPair(file)

		if isDB && desc.IsSkipped(dbID, tableID) {
			delete(filesToUnwrap, file)
		}
	}
}

func (p ExtractProviderDBSpec) TryGetOidPair(file string) (bool, uint32, uint32) {
	if !(strings.HasPrefix(file, defaultTbspPrefix) || strings.HasPrefix(file, customTbspPrefix)) {
		return false, 0, 0
	}
	var tableID, dbID uint32

	file, tableID = p.cutIntegerBase(file)
	_, dbID = p.cutIntegerBase(file)

	return true, dbID, tableID
}

func (p ExtractProviderDBSpec) cutIntegerBase(file string) (string, uint32) {
	parent, base := path.Dir(file), path.Base(file)
	base, _, _ = strings.Cut(base, ".")
	base, _, _ = strings.Cut(base, "_")
	integerResult, _ := strconv.ParseUint(base, 10, 0)

	return parent, uint32(integerResult)
}
