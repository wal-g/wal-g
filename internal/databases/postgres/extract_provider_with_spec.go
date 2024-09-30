package postgres

import (
	"fmt"
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

type RestoreDesc map[uint32]map[uint32]uint32

func (desc RestoreDesc) Add(database, filenode, oid uint32) {
	if _, ok := desc[database]; !ok {
		desc[database] = make(map[uint32]uint32)
	}
	desc[database][filenode] = oid
}

func (desc RestoreDesc) IsFull(database uint32) bool {
	if _, ok := desc[database]; ok {
		_, ok1 := desc[database][0]
		return ok1
	}
	return false
}

func (desc RestoreDesc) IsSkipped(database, tableFile uint32) bool {
	if database < systemIDLimit || desc.IsFull(database) { //TODO not db, but check table oid (now we check relfilenode)
		return false
	}
	if _, ok := desc[database]; ok {
		oid, found := desc[database][tableFile] //seems wrong check
		return oid >= systemIDLimit && !found
	}
	return true
}

func (desc RestoreDesc) FilterFilesToUnwrap(filesToUnwrap map[string]bool) {
	for file := range filesToUnwrap {
		isDB, dbID, tableFileID := TryGetOidPair(file)

		if isDB && desc.IsSkipped(dbID, tableFileID) {
			delete(filesToUnwrap, file)
		}
	}
}

func TryGetOidPair(file string) (bool, uint32, uint32) {
	if !(strings.HasPrefix(file, defaultTbspPrefix) || strings.HasPrefix(file, customTbspPrefix)) {
		return false, 0, 0
	}
	var tableID, dbID uint32

	file, tableID = cutIntegerBase(file)
	_, dbID = cutIntegerBase(file)

	return true, dbID, tableID
}

func cutIntegerBase(file string) (string, uint32) {
	parent, base := path.Dir(file), path.Base(file)
	base, _, _ = strings.Cut(base, ".")
	base, _, _ = strings.Cut(base, "_")
	integerResult, _ := strconv.ParseUint(base, 10, 0)

	return parent, uint32(integerResult)
}

type RestoreDescMaker interface {
	Make(restoreParameters []string, names DatabasesByNames) (RestoreDesc, error)
}

type DefaultRestoreDescMaker struct{}

func (m DefaultRestoreDescMaker) Make(restoreParameters []string, names DatabasesByNames) (RestoreDesc, error) {
	restoredDatabases := make(RestoreDesc)

	for _, parameter := range restoreParameters {
		dbID, tableID, err := names.Resolve(parameter)
		if err != nil {
			return nil, err
		}

		if tableID == 0 {
			restoredDatabases.Add(dbID, tableID, 0)
		} else {
			restoredDatabases.Add(dbID, tableID, names[fmt.Sprintf("%d", dbID)].Tables[fmt.Sprintf("%d", tableID)].Oid)
		}
	}

	return restoredDatabases, nil
}

type RegexpRestoreDescMaker struct{}

func (m RegexpRestoreDescMaker) Make(restoreParameters []string, names DatabasesByNames) (RestoreDesc, error) {
	restoredDatabases := make(RestoreDesc)

	for _, parameter := range restoreParameters {
		oids, err := names.ResolveRegexp(parameter)
		if err != nil {
			return nil, err
		}

		for db, tables := range oids {
			for _, reloid := range tables {
				if reloid == 0 {
					restoredDatabases.Add(db, reloid, 0)
				} else {
					restoredDatabases.Add(db, reloid, names[fmt.Sprintf("%d", db)].Tables[fmt.Sprintf("%d", reloid)].Oid)
				}
			}
		}
	}

	return restoredDatabases, nil
}

type ExtractProviderDBSpec struct {
	RestoreParameters []string
	restoreDescMaker  RestoreDescMaker
}

func NewExtractProviderDBSpec(restoreParameters []string) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{restoreParameters, DefaultRestoreDescMaker{}}
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

	desc, err := p.restoreDescMaker.Make(p.RestoreParameters, filesMeta.DatabasesByNames)
	tracelog.ErrorLogger.FatalOnError(err)
	desc.FilterFilesToUnwrap(filesToUnwrap)

	return ExtractProviderImpl{}.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}
