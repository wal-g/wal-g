package orioledb

import (
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/wal-g/tracelog"
)

var pagedFilenameRegexp *regexp.Regexp

func init() {
	pagedFilenameRegexp = regexp.MustCompile(`^(\d+)([.]\d+)?$`)
}

func IsOrioledbDataPath(filePath string) bool {
	if !strings.Contains(filePath, "orioledb_data") ||
		!pagedFilenameRegexp.MatchString(path.Base(filePath)) {
		return false
	}
	return true
}

func IsOrioledbDataFile(info os.FileInfo, filePath string) bool {
	if info.IsDir() ||
		info.Size() == 0 ||
		!IsOrioledbDataPath(filePath) {
		return false
	}
	return true
}

func IsEnabled(PgDataDirectory string) bool {
	_, err := os.Stat(PgDataDirectory + "/orioledb_data")
	return err == nil
}

func GetChkpNum(PgDataDirectory string) (chkpNum uint32) {
	OrioledbDataDirectory := PgDataDirectory + "/orioledb_data"
	xidRegEx, err := regexp.Compile(`^.+\.(xid)$`)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Cannot compile xid regex")
	}

	chkpNum = uint32(0)
	err = filepath.Walk(OrioledbDataDirectory, func(path string, info os.FileInfo, err error) error {
		if err == nil && xidRegEx.MatchString(info.Name()) {
			xid := strings.Split(info.Name(), ".")[0]
			tempChkpNum, err := strconv.Atoi(xid)
			if err != nil {
				tracelog.ErrorLogger.Fatalf("Cannot parse chkpNum: %s", xid)
			}
			chkpNum = uint32(tempChkpNum)
			return filepath.SkipAll
		}
		if info.IsDir() && filepath.Base(info.Name()) != "orioledb_data" {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Cannot find any xid file")
	}
	return chkpNum
}
