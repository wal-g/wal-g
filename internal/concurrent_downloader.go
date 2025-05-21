package internal

import (
	"fmt"
	"github.com/wal-g/tracelog"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentDownloader struct {
	folder storage.Folder
}

func CreateConcurrentDownloader(uploader Uploader) *ConcurrentDownloader {
	return &ConcurrentDownloader{
		folder: uploader.Folder(),
	}
}

func (downloader *ConcurrentDownloader) Download(backupName, localDirectory string, filter map[string]struct{}) error {
	tarsFolder := downloader.folder.GetSubFolder(strings.Trim(backupName+TarPartitionFolderName, "/"))
	tarsToExtract, err := downloader.getTarsToExtract(tarsFolder, filter)
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

	tarInterpreter := NewFileTarInterpreter(localDirectory)
	return ExtractAll(tarInterpreter, tarsToExtract)
}

func (downloader *ConcurrentDownloader) getTarsToExtract(tarsFolder storage.Folder,
	filter map[string]struct{}) ([]ReaderMaker, error) {
	tarObjects, subFolders, err := tarsFolder.ListFolder()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list '%s'", tarsFolder.GetPath())
	}
	if len(subFolders) > 0 {
		return nil, errors.Wrapf(err, "unknown subfolders in '%s'", tarsFolder.GetPath())
	}

	tarsToExtract := make([]ReaderMaker, 0, len(tarObjects))

	var t []string
	for _, tarObject := range tarObjects {
		t = append(t, tarObject.GetName())
	}

	tracelog.InfoLogger.Printf("TAR OBJECTS %v", t)
	for _, tarObject := range tarObjects {
		if filter != nil && tarObject.GetName() != "part_001.tar.lz4" {
			if _, ok := filter[tarObject.GetName()]; !ok {
				continue
			}
		}
		tarToExtract := NewStorageReaderMaker(tarsFolder, tarObject.GetName())
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}

	return tarsToExtract, nil
}
