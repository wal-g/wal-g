package models

import (
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"strings"
)

type Paths struct {
	DBPath  string `json:"db_path"`
	TarPath string `json:"tar_path"`
}

type IndexInfo map[string]Paths

type CollectionInfo struct {
	Paths     `json:"paths"`
	IndexInfo `json:"index_info"`
}

type DBInfo map[string]CollectionInfo

type BackupRoutesInfo struct {
	Databases map[string]DBInfo `json:"databases"`
	Service   map[string]string `json:"service"`
}

type PathMapFilter map[string]map[string]struct{}

func getFromTarFilesSetAndDeleteKey(key string, tarFilesSet map[string]map[string]struct{}) (string, bool) {
	for tarFile, tarFileSet := range tarFilesSet {
		if _, ok := tarFileSet[key]; ok {
			delete(tarFileSet, key)
			return tarFile, ok
		}
	}
	return "", false
}

func GetSpecialFilesFromTarFilesSet(tarFilesSet map[string]map[string]struct{}) map[string]string {
	res := make(map[string]string)
	for tarFile, tarFileSet := range tarFilesSet {
		for dbFile := range tarFileSet {
			res[dbFile] = tarFile
		}
	}
	return res
}

func EnrichWithTarPaths(backupRoutesInfo *BackupRoutesInfo, tarPaths map[string][]string) error {
	tarFilesSet := map[string]map[string]struct{}{}
	for tarPath, files := range tarPaths {
		tarFilesSet[tarPath] = make(map[string]struct{}, len(files))
		for _, file := range files {
			tarFilesSet[tarPath][file] = struct{}{}
		}
	}

	for dbName, dbInfo := range backupRoutesInfo.Databases {
		for colName, colInfo := range dbInfo {
			colTarPath, ok := getFromTarFilesSetAndDeleteKey(colInfo.DBPath, tarFilesSet)
			if !ok {
				return errors.Errorf("file %s not found in tar directory", colInfo.DBPath)
			}
			colInfo.TarPath = colTarPath

			for indexName, indexInfo := range colInfo.IndexInfo {
				indTarPath, ok := getFromTarFilesSetAndDeleteKey(indexInfo.DBPath, tarFilesSet)
				if !ok {
					return errors.Errorf("file %s not found in tar directory", indexInfo.DBPath)
				}
				indexInfo.TarPath = indTarPath
				colInfo.IndexInfo[indexName] = indexInfo
			}

			backupRoutesInfo.Databases[dbName][colName] = colInfo
		}
	}

	backupRoutesInfo.Service = GetSpecialFilesFromTarFilesSet(tarFilesSet)
	return nil
}

func dbAndColFromURI(uri string) (string, string) {
	if !strings.Contains(uri, ".") {
		return uri, ""
	}

	splitted := strings.SplitN(uri, ".", 2)
	return splitted[0], splitted[1]
}

func getFilters(whitelist, blacklist []string) (map[string]map[string]struct{}, map[string]map[string]struct{}) {
	whitelistFilter := make(map[string]map[string]struct{})
	blacklistFilter := make(map[string]map[string]struct{})

	for _, uri := range whitelist {
		db, col := dbAndColFromURI(uri)

		whitelistFilter[db] = map[string]struct{}{}
		if col != "" {
			whitelistFilter[db][col] = struct{}{}
		}
	}

	whitelistFilter["admin"] = map[string]struct{}{}
	whitelistFilter["local"] = map[string]struct{}{}
	whitelistFilter["config"] = map[string]struct{}{}
	whitelistFilter["mdb_internal"] = map[string]struct{}{}

	for _, uri := range blacklist {
		db, col := dbAndColFromURI(uri)
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
	tracelog.InfoLogger.Printf("whitelist: %v", whitelist)
	tracelog.InfoLogger.Printf("blacklist: %v", blacklist)
	return whitelistFilter, blacklistFilter
}

func shouldDownload(db, col string, whitelist, blacklist map[string]map[string]struct{}, wlSpecified bool) bool {
	nsIn := func(filter map[string]map[string]struct{}, db, col string) bool {
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
	routes *BackupRoutesInfo,
	whitelist []string,
	blacklist []string,
) (map[string]struct{}, map[string]struct{}) {
	tarFilter := make(map[string]struct{})
	pathFilter := make(map[string]struct{})

	whitelistSpecified := len(whitelist) > 0
	whitelistFilter, blacklistFilter := getFilters(whitelist, blacklist)

	for db, dbInfo := range routes.Databases {
		for col, colInfo := range dbInfo {
			if shouldDownload(db, col, whitelistFilter, blacklistFilter, whitelistSpecified) {
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

	tracelog.InfoLogger.Printf("pathFilter: %v", pathFilter)
	tracelog.InfoLogger.Printf("tarFilter: %v", tarFilter)
	return pathFilter, tarFilter
}
