package internal

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DirectoryDownloader interface {
	DownloadDirectory(ctx context.Context, pathToRestore string) error
}

type DirectoryIsNotEmptyError struct {
	error
}

type CommonDirectoryDownloader struct {
	Folder     storage.Folder
	BackupName string
}

func NewCommonDirectoryDownloader(folder storage.Folder, backupName string) DirectoryDownloader {
	return &CommonDirectoryDownloader{folder, backupName}
}

func NewDirectoryIsNotEmptyError(path string) DirectoryIsNotEmptyError {
	return DirectoryIsNotEmptyError{errors.Errorf("Directory %v must have no files", path)}
}

func (downloader *CommonDirectoryDownloader) DownloadDirectory(ctx context.Context, pathToRestore string) error {
	tarsToExtract, err := downloader.getTarsToExtract(ctx)
	if err != nil {
		return err
	}

	isEmpty, err := utility.IsDirectoryEmpty(pathToRestore, nil)
	if err != nil {
		return err
	}

	if !isEmpty {
		return NewDirectoryIsNotEmptyError(pathToRestore)
	}

	return ExtractAll(ctx, NewFileTarInterpreter(pathToRestore), tarsToExtract)
}

func (downloader *CommonDirectoryDownloader) getTarPartitionFolder() storage.Folder {
	return downloader.Folder.GetSubFolder(downloader.BackupName + TarPartitionFolderName)
}

func (downloader *CommonDirectoryDownloader) getTarNames(ctx context.Context) (names []string, err error) {
	tarPartitionFolder := downloader.getTarPartitionFolder()
	objects, _, err := tarPartitionFolder.ListFolder(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to list backup '%s' for deletion", downloader.BackupName)
	}

	result := make([]string, len(objects))
	for id, object := range objects {
		result[id] = object.GetName()
	}

	return result, nil
}

func (downloader *CommonDirectoryDownloader) getTarsToExtract(ctx context.Context) (tarsToExtract []ReaderMaker, err error) {
	tarNames, err := downloader.getTarNames(ctx)
	if err != nil {
		return nil, err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]ReaderMaker, 0, len(tarNames))

	for _, tarName := range tarNames {
		tarToExtract := NewStorageReaderMaker(downloader.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}

	return tarsToExtract, nil
}
