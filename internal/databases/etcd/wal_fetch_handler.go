package etcd

import (
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type SentinelDto struct {
	StartLocalTime time.Time `json:"StartLocalTime,omitempty"`
}

func HandleWalFetch(folder storage.Folder, backupName string, dstDir string, baseReader internal.StorageFolderReader) {
	reader := baseReader.SubFolder(utility.WalPath)

	backup, err := internal.GetBackupByName(internal.LatestString, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get mentioned backup: %v", err)

	var lastBackupSentinel SentinelDto
	err = backup.FetchSentinel(&lastBackupSentinel)
	tracelog.ErrorLogger.FatalfOnError("Failed to unmarshall backup sentinel: %v", err)

	walFiles, _, err := folder.GetSubFolder(utility.WalPath).ListFolder()
	tracelog.ErrorLogger.FatalfOnError("Failed to list wal folder from storage: %v", err)
	fmt.Println(walFiles)

	sort.Slice(walFiles, func(i, j int) bool {
		return walFiles[i].GetLastModified().Before(walFiles[j].GetLastModified())
	})

	for _, walFile := range walFiles {
		if lastBackupSentinel.StartLocalTime.Before(walFile.GetLastModified()) {
			walName := strings.TrimSuffix(walFile.GetName(), filepath.Ext(walFile.GetName()))
			walPath := path.Join(dstDir, walName)
			tracelog.InfoLogger.Printf("fetching %s into %s", walName, walPath)
			err = internal.DownloadFileTo(reader, walName, walPath)
			tracelog.ErrorLogger.FatalfOnError("Failed to download wal file: %v", err)
		}
	}
}
