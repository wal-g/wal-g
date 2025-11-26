package partial

import (
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type SetMap map[string]map[string]struct{}

func getFromTarFilesSet(
	key string,
	tarFilesSet SetMap,
	visited map[string]struct{},
) (string, bool) {
	for tarFile, tarFileSet := range tarFilesSet {
		if _, ok := tarFileSet[key]; ok {
			visited[tarFile] = struct{}{}
			return tarFile, ok
		}
	}
	return "", false
}

func EnrichWithTarPaths(backupRoutesInfo *models.BackupRoutesInfo, tarPaths map[string][]string) error {
	tarFilesSet := make(SetMap)
	for tarPath, files := range tarPaths {
		tarFilesSet[tarPath] = make(map[string]struct{}, len(files))
		for _, file := range files {
			tarFilesSet[tarPath][file] = struct{}{}
		}
	}
	visited := make(map[string]struct{})

	for dbName, dbInfo := range backupRoutesInfo.Databases {
		for colName, colInfo := range dbInfo {
			colTarPath, ok := getFromTarFilesSet(colInfo.DBPath, tarFilesSet, visited)
			if !ok {
				return errors.Errorf("file %s not found in tar directory", colInfo.DBPath)
			}
			colInfo.TarPath = colTarPath

			for indexName, indexInfo := range colInfo.IndexInfo {
				indTarPath, ok := getFromTarFilesSet(indexInfo.DBPath, tarFilesSet, visited)
				if !ok {
					return errors.Errorf("file %s not found in tar directory", indexInfo.DBPath)
				}
				indexInfo.TarPath = indTarPath
				colInfo.IndexInfo[indexName] = indexInfo
			}

			backupRoutesInfo.Databases[dbName][colName] = colInfo
		}
	}

	for tarFile, tarFileSet := range tarFilesSet {
		if _, ok := visited[tarFile]; !ok {
			for dbFile := range tarFileSet {
				backupRoutesInfo.Service[dbFile] = tarFile
			}
		}
	}

	return nil
}

func GetFilters(whitelist, blacklist []string) (map[string]map[string]struct{}, map[string]map[string]struct{}) {
	whitelistFilter := make(SetMap)
	blacklistFilter := make(SetMap)

	if len(whitelist)+len(blacklist) == 0 {
		return whitelistFilter, blacklistFilter
	}

	for _, uri := range whitelist {
		db, col := util.SplitNamespace(uri)

		if _, ok := whitelistFilter[db]; !ok {
			whitelistFilter[db] = map[string]struct{}{}
		}

		if col != "" {
			whitelistFilter[db][col] = struct{}{}
		}
	}

	whitelistFilter["admin"] = map[string]struct{}{}
	whitelistFilter["local"] = map[string]struct{}{}
	whitelistFilter["config"] = map[string]struct{}{}
	whitelistFilter["mdb_internal"] = map[string]struct{}{}

	for _, uri := range blacklist {
		db, col := util.SplitNamespace(uri)
		delete(whitelistFilter[db], col)

		if _, ok := blacklistFilter[db]; !ok {
			blacklistFilter[db] = map[string]struct{}{}
		}

		if col != "" {
			blacklistFilter[db][col] = struct{}{}
		} else {
			delete(whitelistFilter, db)
		}
	}

	return whitelistFilter, blacklistFilter
}

func ShouldDownload(db, col string, whitelist, blacklist SetMap, wlSpecified bool) bool {
	nsIn := func(filter SetMap, db, col string) bool {
		cols, dbOk := filter[db]
		if dbOk && len(cols) == 0 {
			return true
		}
		_, ok := filter[db][col]
		return ok
	}

	if wlSpecified {
		if nsIn(whitelist, db, col) {
			return !nsIn(blacklist, db, col)
		}
		return false
	}

	return !nsIn(blacklist, db, col)
}

func GetTarFilesFilter(
	routes *models.BackupRoutesInfo,
	whitelist []string,
	blacklist []string,
) (map[string]struct{}, map[string]struct{}) {
	tarFilter := make(map[string]struct{})
	pathFilter := make(map[string]struct{})

	whitelistSpecified := len(whitelist) > 0
	whitelistFilter, blacklistFilter := GetFilters(whitelist, blacklist)
	tracelog.DebugLogger.Printf("Whitelist namespaces filter: %v", whitelistFilter)
	tracelog.DebugLogger.Printf("Blacklist namespaces filter: %v", blacklistFilter)

	for db, dbInfo := range routes.Databases {
		for col, colInfo := range dbInfo {
			if ShouldDownload(db, col, whitelistFilter, blacklistFilter, whitelistSpecified) {
				tarFilter[colInfo.TarPath] = struct{}{}
				pathFilter[colInfo.DBPath] = struct{}{}
				for _, indexPaths := range colInfo.IndexInfo {
					tarFilter[indexPaths.TarPath] = struct{}{}
					pathFilter[indexPaths.DBPath] = struct{}{}
				}
			}
		}
	}

	for dbFile, tarFile := range routes.Service {
		tarFilter[tarFile] = struct{}{}
		pathFilter[dbFile] = struct{}{}
	}

	return pathFilter, tarFilter
}
