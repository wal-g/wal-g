package transfer

import (
	"path"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type BackupFileLister struct {
	Name       string
	Overwrite  bool
	MaxFiles   int
	MaxBackups int
}

const prefix = utility.BaseBackupPath

func NewSingleBackupFileLister(name string, overwrite bool, maxFiles int) *BackupFileLister {
	return &BackupFileLister{
		Name:       name,
		Overwrite:  overwrite,
		MaxFiles:   maxFiles,
		MaxBackups: 1,
	}
}

func NewAllBackupsFileLister(overwrite bool, maxFiles, maxBackups int) *BackupFileLister {
	return &BackupFileLister{
		Name:       "",
		Overwrite:  overwrite,
		MaxFiles:   maxFiles,
		MaxBackups: maxBackups,
	}
}

func (l *BackupFileLister) ListFilesToMove(source, target storage.Folder) (files []FilesGroup, num int, err error) {
	missingFiles, err := listMissingFiles(source, target, prefix, l.Overwrite)
	if err != nil {
		return nil, 0, err
	}
	backups := findBackups(missingFiles, l.Name)
	fileGroups, filesNum := groupAndLimitBackupFiles(backups, l.MaxFiles, l.MaxBackups)
	return fileGroups, filesNum, nil
}

type backupFiles struct {
	sentinel   storage.Object
	backupData []storage.Object
}

func findBackups(files map[string]storage.Object, targetName string) map[string]backupFiles {
	backups := map[string]backupFiles{}
	for filePath, file := range files {
		category, backupName := categoriseFile(filePath)
		if category == fileCategoryOther {
			continue
		}
		if targetName != "" && targetName != backupName {
			continue
		}
		backup := backups[backupName]
		switch category {
		case fileCategorySentinel:
			backup.sentinel = file
		case fileCategoryBackupData:
			backup.backupData = append(backup.backupData, file)
		}
		backups[backupName] = backup
	}
	tracelog.InfoLogger.Printf("Backups missing in the target storage: %d", len(backups))
	return backups
}

type fileCategory int

const (
	fileCategoryOther fileCategory = iota
	fileCategorySentinel
	fileCategoryBackupData
)

func categoriseFile(filePath string) (category fileCategory, backupName string) {
	dir, fileName := path.Split(strings.TrimPrefix(filePath, prefix))
	if dir == "" &&
		strings.HasPrefix(fileName, utility.BackupNamePrefix) &&
		strings.HasSuffix(fileName, utility.SentinelSuffix) {
		backupName = strings.TrimSuffix(fileName, utility.SentinelSuffix)
		return fileCategorySentinel, backupName
	}
	if strings.HasPrefix(dir, utility.BackupNamePrefix) {
		firstSlash := strings.Index(dir, "/")
		return fileCategoryBackupData, dir[:firstSlash]
	}
	return fileCategoryOther, ""
}

func groupAndLimitBackupFiles(backups map[string]backupFiles, maxFiles, maxBackups int) (files []FilesGroup, num int) {
	filesCount := 0
	fileGroups := make([]FilesGroup, 0, len(backups))
	for name, backup := range backups {
		if backup.sentinel == nil {
			tracelog.InfoLogger.Printf("Skip incomplete backup without sentinel file: %s", name)
			continue
		}
		if len(backup.backupData) == 0 {
			tracelog.WarningLogger.Printf("Backup doesn't have any data: %s", name)
			continue
		}

		group := linkGroup(backup)

		if filesCount+len(group) > maxFiles {
			break
		}
		filesCount += len(group)

		fileGroups = append(fileGroups, group)
		if len(fileGroups) >= maxBackups {
			break
		}
	}
	tracelog.InfoLogger.Printf("Backups will be transferred: %d", len(fileGroups))
	tracelog.InfoLogger.Printf("Files will be transferred: %d", filesCount)
	return fileGroups, filesCount
}

// linkGroup makes a linked group of files from the backup. The sentinel file is linked to data files so that it will be
// copied to the target storage only after them. In turn, data files are linked to the sentinel file so that they will
// be deleted from the source storage only after it. This is needed to move backups consistently and atomically.
func linkGroup(backup backupFiles) FilesGroup {
	sentinelFile := FileToMove{
		path: backup.sentinel.GetName(),
	}
	filesToMove := make([]FileToMove, 0, len(backup.backupData))
	for _, obj := range backup.backupData {
		dataFile := FileToMove{
			path:        obj.GetName(),
			deleteAfter: []string{sentinelFile.path},
		}
		filesToMove = append(filesToMove, dataFile)
		sentinelFile.copyAfter = append(sentinelFile.copyAfter, dataFile.path)
	}

	return append(filesToMove, sentinelFile)
}
