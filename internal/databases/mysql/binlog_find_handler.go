package mysql

import (
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/tracelog"
)

func HandleBinlogFind(folder storage.Folder, gtid string) {
	db, err := getMySQLConnection()
	logging.FatalOnError(err)
	defer utility.LoggedClose(db, "")
	flavor, err := getMySQLFlavor(db)
	logging.FatalOnError(err)
	var gtidSet gomysql.GTIDSet
	if gtid == "" {
		gtidSet, err = getMySQLGTIDExecuted(db, flavor)
		logging.FatalOnError(err)
	} else {
		gtidSet, err = gomysql.ParseGTIDSet(flavor, gtid)
		logging.FatalOnError(err)
	}
	name, err := getLastUploadedBinlogBeforeGTID(folder, gtidSet, flavor)
	logging.FatalOnError(err)
	tracelog.InfoLogger.Println(name)
}
