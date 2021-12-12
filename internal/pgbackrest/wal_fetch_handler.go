package pgbackrest

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
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
		if strings.HasPrefix(file.GetName(), walFileName) {
			return internal.DownloadFileTo(walFolder, file.GetName(), location)
		}
	}

	return errors.New("File " + walFileName + " not found in storage")
}