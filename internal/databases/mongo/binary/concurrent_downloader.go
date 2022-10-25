package binary

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentDownloader struct {
	folder storage.Folder
}

func CreateConcurrentDownloader(uploader *internal.Uploader) *ConcurrentDownloader {
	return &ConcurrentDownloader{
		folder: uploader.Folder(),
	}
}

func (downloader *ConcurrentDownloader) Download(backupName, localDirectory string) error {
	tarsFolder := downloader.folder.GetSubFolder(strings.Trim(backupName+internal.TarPartitionFolderName, "/"))
	tarsToExtract, err := downloader.getTarsToExtract(tarsFolder)
	if err != nil {
		return err
	}

	isEmpty, err := utility.IsDirectoryEmpty(localDirectory)
	if err != nil {
		return err
	}
	if !isEmpty {
		return fmt.Errorf("directory '%s' should be empty", localDirectory)
	}

	tarInterpreter := internal.NewFileTarInterpreter(localDirectory)
	return internal.ExtractAll(tarInterpreter, tarsToExtract)
}

func (downloader *ConcurrentDownloader) getTarsToExtract(tarsFolder storage.Folder) ([]internal.ReaderMaker, error) {
	tarObjects, subFolders, err := tarsFolder.ListFolder()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list '%s'", tarsFolder.GetPath())
	}
	if len(subFolders) > 0 {
		return nil, errors.Wrapf(err, "unknown subfolders in '%s'", tarsFolder.GetPath())
	}

	tarsToExtract := make([]internal.ReaderMaker, 0, len(tarObjects))

	for _, tarObject := range tarObjects {
		tarToExtract := internal.NewStorageReaderMaker(tarsFolder, tarObject.GetName())
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}

	return tarsToExtract, nil
}
