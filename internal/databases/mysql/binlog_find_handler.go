package mysql

import (
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

func HandleBinlogFind(folder storage.Folder, gtid string) {
	db, err := getMySQLConnection()
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(db, "")
	flavor, err := getMySQLFlavor(db)
	tracelog.ErrorLogger.FatalOnError(err)
	if gtid == "" {
		gtid, err = getMySQLGTIDExecuted(db, flavor)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	name, err := getLastUploadedBinlogBeforeGTID(folder, gtid, flavor)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println(name)
}
