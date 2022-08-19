package pgbackrest

import (
	"errors"
	"os"
	"path"
	"path/filepath"

	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandlePgbackrestBackupFetch(folder storage.Folder, stanza string, destinationDirectory string,
	backupSelector internal.BackupSelector) error {
	backupName, err := backupSelector.Select(folder)
	if err != nil {
		return err
	}

	backupDetails, err := GetBackupDetails(folder, stanza, backupName)
	if err != nil {
		return err
	}

	switch backupDetails.Type {
	case "full":
		return fullBackupFetch(folder, stanza, backupName, destinationDirectory, backupDetails)
	default:
		return errors.New("Unsupported backup type: " + backupDetails.Type)
	}
}

func fullBackupFetch(folder storage.Folder, stanza string, backupName string,
	destinationDirectory string, backupDetails *BackupDetails) error {
	backupFilesFolder := folder.GetSubFolder(BackupFolderName).GetSubFolder(stanza).GetSubFolder(backupName).GetSubFolder(BackupDataDirectory)
	err := createDirectories(backupDetails, destinationDirectory)
	if err != nil {
		return err
	}

	files, err := getFilesRecursively(backupFilesFolder, backupFilesFolder, backupDetails.DefaultFileMode)
	if err != nil {
		return err
	}

	fileInterpreter := postgres.NewFileTarInterpreter(destinationDirectory, postgres.BackupSentinelDto{},
		postgres.FilesMetadataDto{}, getFilesToUnwrap(files), false)
	return internal.ExtractAll(fileInterpreter, files)
}

func getFilesToUnwrap(files []internal.ReaderMaker) map[string]bool {
	filesToUnwrap := make(map[string]bool)
	for _, file := range files {
		filesToUnwrap[file.LocalPath()] = true
	}
	return filesToUnwrap
}

func createDirectories(backupDetails *BackupDetails, dbDataDirectory string) error {
	for _, directoryPath := range backupDetails.DirectoryPaths {
		relativeDirectory, err := filepath.Rel(BackupDataDirectory, directoryPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(filepath.Join(dbDataDirectory, relativeDirectory), os.FileMode(backupDetails.DefaultDirectoryMode))
		if err != nil {
			return err
		}
	}
	return nil
}

func getFilesRecursively(folder storage.Folder, backupFilesFolder storage.Folder, fileMode int) (files []internal.ReaderMaker, err error) {
	objects, subfolders, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}

	for _, object := range objects {
		relativePath, err := filepath.Rel(backupFilesFolder.GetPath(), folder.GetPath())
		if err != nil {
			return nil, err
		}
		filePath := path.Join(relativePath, object.GetName())
		file := internal.NewRegularFileStorageReaderMarker(backupFilesFolder, filePath, utility.TrimFileExtension(filePath), int64(fileMode))
		files = append(files, file)
	}

	for _, subfolder := range subfolders {
		subfolderFiles, err := getFilesRecursively(subfolder, backupFilesFolder, fileMode)
		if err != nil {
			return nil, err
		}
		files = append(files, subfolderFiles...)
	}
	return files, err
}
