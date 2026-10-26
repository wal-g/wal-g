package internal

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type ConcurrentDownloader struct {
	folder    storage.Folder
	whitelist *regexp.Regexp
}

func CreateConcurrentDownloader(uploader Uploader, whitelist *regexp.Regexp) *ConcurrentDownloader {
	return &ConcurrentDownloader{
		folder:    uploader.Folder(),
		whitelist: whitelist,
	}
}

func (downloader *ConcurrentDownloader) Download(ctx context.Context, backupName, localDirectory string, filter map[string]struct{}) error {
	return downloader.DownloadWithTarInterpreter(
		ctx, backupName, localDirectory, filter, NewFileTarInterpreter(localDirectory),
	)
}

func (downloader *ConcurrentDownloader) DownloadWithTarInterpreter(ctx context.Context,
	backupName, localDirectory string, filter map[string]struct{}, tarInterpreter TarInterpreter,
) error {
	tarsFolder := downloader.folder.GetSubFolder(strings.Trim(backupName+TarPartitionFolderName, "/"))
	tarsToExtract, err := downloader.getTarsToExtract(ctx, tarsFolder, filter)
	if err != nil {
		return err
	}

	isEmpty, err := utility.IsDirectoryEmpty(localDirectory, downloader.whitelist)
	if err != nil {
		return err
	}
	if !isEmpty {
		return fmt.Errorf("directory '%s' should be empty", localDirectory)
	}

	if err := ExtractAll(ctx, tarInterpreter, tarsToExtract); err != nil {
		return err
	}
	if finalizer, ok := tarInterpreter.(finalizingTarInterpreter); ok {
		return finalizer.Finalize()
	}
	return nil
}

func (downloader *ConcurrentDownloader) getTarsToExtract(ctx context.Context,
	tarsFolder storage.Folder, filter map[string]struct{}) ([]ReaderMaker, error) {
	if filter != nil {
		tarNames := make([]string, 0, len(filter))
		for tarName := range filter {
			if tarName == "" || tarName == "." || tarName == ".." || strings.ContainsAny(tarName, `/\`) {
				return nil, errors.Errorf("invalid tar object name %q in filter", tarName)
			}
			tarNames = append(tarNames, tarName)
		}
		sort.Strings(tarNames)

		tarsToExtract := make([]ReaderMaker, 0, len(tarNames))
		for _, tarName := range tarNames {
			tarsToExtract = append(tarsToExtract, NewStorageReaderMaker(tarsFolder, tarName))
		}
		return tarsToExtract, nil
	}

	tarObjects, subFolders, err := tarsFolder.ListFolder(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list '%s'", tarsFolder.GetPath())
	}
	if len(subFolders) > 0 {
		return nil, errors.Errorf("unknown subfolders in '%s'", tarsFolder.GetPath())
	}

	tarsToExtract := make([]ReaderMaker, 0, len(tarObjects))

	for _, tarObject := range tarObjects {
		tarToExtract := NewStorageReaderMaker(tarsFolder, tarObject.GetName())
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}

	return tarsToExtract, nil
}
