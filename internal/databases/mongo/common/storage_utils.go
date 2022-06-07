package common

import (
	"fmt"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func DownloadSentinel(folder storage.Folder, backupName string, sentinel interface{}) error {
	internalNewBackup := internal.NewBackup(folder, backupName)
	err := internalNewBackup.FetchSentinel(sentinel)
	if err != nil {
		return fmt.Errorf("can not fetch stream sentinel: %w", err)
	}
	return nil
}

func GetBackupFolder() (backupFolder storage.Folder, err error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, err
	}
	return folder.GetSubFolder(utility.BaseBackupPath), err
}
