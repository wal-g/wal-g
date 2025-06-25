package models

import (
	"github.com/pkg/errors"
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

func PartialWhitelistPathsMap(paths []string) map[string][]string {
	res := make(map[string][]string)

	for _, path := range paths {
		if !strings.Contains(path, ".") {
			res[path] = []string{}
		} else {
			splitted := strings.SplitN(path, ".", 2)
			db, col := splitted[0], splitted[1]

			if _, ok := res[db]; !ok {
				res[db] = []string{}
			}
			res[db] = append(res[db], col)
		}
	}

	res["admin"] = []string{}
	res["local"] = []string{}

	return res
}

func PartialBlacklistPathMap(paths []string) map[string]map[string]struct{} {
	res := make(map[string]map[string]struct{})

	for _, path := range paths {
		if !strings.Contains(path, ".") {
			res[path] = map[string]struct{}{}
		} else {
			splitted := strings.SplitN(path, ".", 2)
			db, col := splitted[0], splitted[1]

			if _, ok := res[db]; !ok {
				res[db] = map[string]struct{}{}
			}
			res[db][col] = struct{}{}
		}
	}

	return res
}

func GetTarFilesFilter(
	routes *BackupRoutesInfo,
	whitelist map[string][]string,
	blacklist map[string]map[string]struct{},
) (map[string]struct{}, map[string]struct{}, error) {
	tarFilter := make(map[string]struct{})
	pathFilter := make(map[string]struct{})

	for db, cols := range whitelist {
		if _, ok := routes.Databases[db]; !ok {
			return nil, nil, errors.Errorf("No db %s in backup", db)
		}
		blacklistMap, blDBOk := blacklist[db]
		if blDBOk && len(blacklistMap) == 0 {
			continue
		}

		colsToIterate := cols
		if len(cols) == 0 {
			colsToIterate = make([]string, 0, len(routes.Databases[db]))
			for k := range routes.Databases[db] {
				colsToIterate = append(colsToIterate, k)
			}
		}

		for _, col := range colsToIterate {
			if blDBOk {
				if _, ok := blacklistMap[col]; ok {
					continue
				}
			}
			colInfo, ok := routes.Databases[db][col]
			if !ok {
				return nil, nil, errors.Errorf("No collection %s in db %s in backup", col, db)
			}

			tarFilter[colInfo.TarPath] = struct{}{}
			pathFilter[colInfo.DBPath] = struct{}{}
			for _, indPaths := range colInfo.IndexInfo {
				tarFilter[indPaths.TarPath] = struct{}{}
				pathFilter[indPaths.DBPath] = struct{}{}
			}
		}
	}

	for dbFile, tarFile := range routes.Service {
		tarFilter[tarFile] = struct{}{}
		pathFilter[dbFile] = struct{}{}
	}

	return pathFilter, tarFilter, nil
}
