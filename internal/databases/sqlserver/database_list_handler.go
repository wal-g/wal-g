package sqlserver

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

func HandleDatabaseList(backupName string) {
	storage, err := internal.ConfigureStorage()
	tracelog.ErrorLogger.FatalOnError(err)
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, storage.RootFolder())
	if err != nil {
		tracelog.ErrorLogger.Fatalf("can't find backup %s: %v", backupName, err)
	}
	sentinel := new(SentinelDto)
	err = backup.FetchSentinel(sentinel)
	tracelog.ErrorLogger.FatalOnError(err)
	for _, name := range sentinel.Databases {
		fmt.Println(name)
	}
}
