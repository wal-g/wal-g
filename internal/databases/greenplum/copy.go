package greenplum

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// HandleCopy copy specific or all backups from one storage to another
func HandleCopy(fromConfigFile string, toConfigFile string, backupName string) {
	var from, fromError = internal.StorageFromConfig(fromConfigFile)
	var to, toError = internal.StorageFromConfig(toConfigFile)
	if fromError != nil || toError != nil {
		return
	}
	infos, err := GetCopyingInfos(backupName, from.RootFolder(), to.RootFolder())
	tracelog.ErrorLogger.FatalOnError(err)
	err = copy.Infos(infos)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Success copy.")
}

func GetCopyingInfos(backupName string,
	from storage.Folder,
	to storage.Folder) ([]copy.InfoProvider, error) {
	tracelog.InfoLogger.Printf("Handle backupname '%s'.", backupName)
	backup, err := internal.GetBackupByName(backupName, utility.BaseBackupPath, from)
	if err != nil {
		return nil, err
	}

	pgBackup := postgres.ToPgBackup(backup)
	backupInfo, err := postgres.BackupCopyingInfo(pgBackup, from, to)
	if err != nil {
		return nil, err
	}
	infos := []copy.InfoProvider{}
	infos = append(infos, backupInfo...)

	var sentinel BackupSentinelDto
	err = backup.FetchSentinel(&sentinel)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to get backup %s", backupName)
		return nil, err
	}

	for _, meta := range sentinel.Segments {
		fromSubfolder := from.GetSubFolder(fmt.Sprintf("%s/seg%d/", utility.SegmentsPath, meta.ContentID))
		toSubfolder := to.GetSubFolder(fmt.Sprintf("%s/seg%d/", utility.SegmentsPath, meta.ContentID))
		backup, err := internal.GetBackupByName(meta.BackupName,
			fmt.Sprintf("%s/seg%d/%s", utility.SegmentsPath, meta.ContentID, utility.BaseBackupPath), from)
		if err != nil {
			return nil, err
		}
		pgBackup := postgres.ToPgBackup(backup)

		backupInfo, err := postgres.BackupCopyingInfo(pgBackup, fromSubfolder, toSubfolder)
		if err != nil {
			return nil, err
		}
		infos = append(infos, backupInfo...)
		historyInfo, err := postgres.HistoryCopyingInfo(pgBackup, fromSubfolder, toSubfolder, false)
		if err != nil {
			return nil, err
		}

		infos = append(infos, historyInfo...)
	}

	return infos, nil
}
