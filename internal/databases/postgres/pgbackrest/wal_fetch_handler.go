package pgbackrest

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleWalFetch(folder storage.Folder, stanza string, walFileName string, location string) error {
	archiveName, err := GetArchiveName(folder, stanza)
	if err != nil {
		return err
	}

	archiveFolder := folder.GetSubFolder(WalArchivePath).GetSubFolder(stanza).GetSubFolder(*archiveName)
	if strings.HasSuffix(walFileName, ".history") {
		return internal.DownloadFileTo(archiveFolder, walFileName, location)
	}

	subdirectoryName := walFileName[0:16]
	walFolder := archiveFolder.GetSubFolder(subdirectoryName)
	if strings.HasSuffix(walFileName, ".backup") {
		return internal.DownloadFileTo(walFolder, walFileName, location)
	}
	fileList, _, err := walFolder.ListFolder()
	if err != nil {
		return err
	}

	for _, file := range fileList {
		fileName := file.GetName()
		if strings.HasPrefix(fileName, walFileName) {
			return internal.DownloadFileTo(walFolder, strings.TrimSuffix(fileName, filepath.Ext(fileName)), location)
		}
	}

	return errors.New("File " + walFileName + " not found in storage")
}
