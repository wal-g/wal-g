package mysql

import (
	"context"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleBinlogFind(ctx context.Context, folder storage.Folder, gtid string) {
	conn, err := getMySQLConnection(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(conn, "")
	flavor, err := getMySQLFlavor(conn)
	tracelog.ErrorLogger.FatalOnError(err)
	var gtidSet gomysql.GTIDSet
	if gtid == "" {
		gtidSet, err = getMySQLGTIDExecuted(conn, flavor)
		tracelog.ErrorLogger.FatalOnError(err)
	} else {
		gtidSet, err = gomysql.ParseGTIDSet(flavor, gtid)
		tracelog.ErrorLogger.FatalOnError(err)
	}
	name, err := getLastUploadedBinlogBeforeGTID(ctx, folder, gtidSet, flavor)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println(name)
}
