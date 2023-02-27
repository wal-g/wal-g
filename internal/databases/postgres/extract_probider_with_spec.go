package postgres

import (
	"path"
	"strconv"
	"strings"

	"github.com/wal-g/wal-g/internal"
)

const (
	defaultTbspPrefix = "/" + DefaultTablespace + "/"
)

type ExtractProviderDBSpec struct {
	ExtractProviderImpl
	onlyDatabases []int
}

func NewExtractProviderDBSpec(onlyDatabases []int) *ExtractProviderDBSpec {
	return &ExtractProviderDBSpec{onlyDatabases: onlyDatabases}
}

func (t ExtractProviderDBSpec) Get(
	backup Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (IncrementalTarInterpreter, []internal.ReaderMaker, string, error) {
	err := t.filterFilesToUnwrap(filesToUnwrap, t.makeRestorePatterns(t.onlyDatabases))
	if err != nil {
		return nil, nil, "", err
	}

	return t.ExtractProviderImpl.Get(backup, filesToUnwrap, skipRedundantTars, dbDataDir, createNewIncrementalFiles)
}

func (t ExtractProviderDBSpec) makeRestorePatterns(databases []int) []string {
	restorePatterns := make([]string, 0)

	for _, id := range databases {
		restorePatterns = append(restorePatterns, defaultTbspPrefix+strconv.Itoa(id)+"/*")
	}

	return restorePatterns
}

func (t ExtractProviderDBSpec) filterFilesToUnwrap(filesToUnwrap map[string]bool, restorePatterns []string) error {
	for file := range filesToUnwrap {
		if !strings.HasPrefix(file, defaultTbspPrefix) {
			continue
		}

		inPatterns, err := t.isFileInPatterns(restorePatterns, file)
		if err != nil {
			return err
		}
		if !inPatterns {
			delete(filesToUnwrap, file)
		}
	}

	return nil
}

func (t ExtractProviderDBSpec) isFileInPatterns(restorePatterns []string, file string) (bool, error) {
	inPatterns := false
	for _, pattern := range restorePatterns {
		res, err := path.Match(pattern, file)
		if err != nil {
			return false, err
		}
		if res {
			inPatterns = true
			break
		}
	}
	return inPatterns, nil
}
