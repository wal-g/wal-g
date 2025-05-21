package models

import (
	"fmt"
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

type DbInfo map[string]CollectionInfo

type BackupRoutesInfo struct {
	Databases map[string]DbInfo `json:"databases"`
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
	tracelog.InfoLogger.Printf("tarPaths:, %v", tarPaths)
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
				return errors.New(fmt.Sprintf("file %s not found in tar directory", colInfo.DBPath))
			}
			colInfo.TarPath = colTarPath

			for indexName, indexInfo := range colInfo.IndexInfo {
				indTarPath, ok := getFromTarFilesSetAndDeleteKey(indexInfo.DBPath, tarFilesSet)
				if !ok {
					return errors.New(fmt.Sprintf("file %s not found in tar directory", indexInfo.DBPath))
				}
				indexInfo.TarPath = indTarPath
				colInfo.IndexInfo[indexName] = indexInfo
			}

			backupRoutesInfo.Databases[dbName][colName] = colInfo
		}
	}

	tracelog.InfoLogger.Printf("backups, %v", backupRoutesInfo)
	tracelog.InfoLogger.Printf("ENDtarFilesSetEND:, %v", tarFilesSet)
	backupRoutesInfo.Service = GetSpecialFilesFromTarFilesSet(tarFilesSet)
	tracelog.InfoLogger.Printf("FINALtarFilesSetFINAL:, %v", tarFilesSet)
	return nil
}

func PartiallyPathsMap(paths []string) map[string][]string {
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

	return res
}

func GetTarFilesFilter(routes BackupRoutesInfo, partially map[string][]string) (map[string]struct{}, map[string]struct{}, error) {
	tarFilter := make(map[string]struct{})
	pathFilter := make(map[string]struct{})

	for db, cols := range partially {
		if _, ok := routes.Databases[db]; !ok {
			return nil, nil, errors.Errorf("No db %s in backup", db)
		}

		for _, col := range cols {
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
