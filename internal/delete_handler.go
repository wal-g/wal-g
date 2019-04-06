package internal

import (
	"fmt"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"sort"
	"strings"
	"time"
)

const (
	NoDeleteModifier = iota
	FullDeleteModifier
	FindFullDeleteModifier
)

// TODO : unit tests
func adjustDeleteTarget(target *Backup, findFull bool) (*Backup, error) {
	sentinelDto, err := target.FetchSentinel()
	if err != nil {
		return nil, err
	}
	if sentinelDto.isIncremental() {
		if findFull {
			target.Name = *sentinelDto.IncrementFullName
		} else {
			errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
			return nil, NewForbiddenActionError(fmt.Sprintf(errorMessage, target.Name))
		}
	}
	return target, nil
}

// TODO : unit tests
func HandleDeleteRetain(folder storage.Folder, retantionCount int, modifier int, dryRun bool) {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)

	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	if modifier == FullDeleteModifier {
		if len(backups) <= retantionCount {
			tracelog.WarningLogger.Printf("Have only %v backups.\n", len(backups))
		}
		left := retantionCount
		for _, b := range backups {
			if left == 1 {
				err = deleteBeforeTarget(folder, NewBackup(baseBackupFolder, b.BackupName), true, dryRun)
				if err != nil {
					tracelog.ErrorLogger.FatalError(err)
				}
				return
			}
			backup := NewBackup(baseBackupFolder, b.BackupName)
			dto, err := backup.FetchSentinel()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			if !dto.isIncremental() {
				left--
			}
		}
		tracelog.WarningLogger.Printf("Scanned all backups but didn't have %v full.", retantionCount)
	} else {
		if len(backups) <= retantionCount {
			tracelog.WarningLogger.Printf("Have only %v backups.\n", len(backups))
		} else {
			err = deleteBeforeTarget(folder, NewBackup(baseBackupFolder, backups[retantionCount-1].BackupName), modifier == FindFullDeleteModifier, dryRun)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
		}
	}
}

// TODO : unit tests
func HandleDeleteBeforeTime(folder storage.Folder, before time.Time, modifier int, dryRun bool) {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)

	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	for _, b := range backups {
		if b.Time.Before(before) {
			err = deleteBeforeTarget(folder, NewBackup(baseBackupFolder, b.BackupName), modifier == FindFullDeleteModifier, dryRun)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			return
		}
	}
	tracelog.WarningLogger.Println("No backups before ", before)
}

// TODO : unit tests
func deleteBeforeTarget(folder storage.Folder, target *Backup, findFull, dryRun bool) error {
	target, err := adjustDeleteTarget(target, findFull)
	if err != nil {
		return err
	}
	walFolder := folder.GetSubFolder(WalPath)
	backupToScan, garbage, err := getBackupsAndGarbage(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	garbageToDelete := findGarbageToDelete(garbage, target)

	skipLine, walSkipFileName := ComputeDeletionSkiplineAndPrintIntentions(backupToScan, target)

	if dryRun { // TODO : split this function by two: 'find objects to delete' and 'delete these objects'
		tracelog.InfoLogger.Printf("Dry run finished.\n")
		return nil
	}

	for _, garbageName := range garbageToDelete {
		dropBackup(folder, garbageName)
	}
	if skipLine < len(backupToScan)-1 {
		DeleteWALBefore(walSkipFileName, walFolder)
		deleteBackupsBefore(backupToScan, skipLine, folder)
		for _, extension := range Extensions {
			extension.Flush(backupToScan[skipLine], folder)
		}
	}
	return nil
}

// TODO : unit tests
func findGarbageToDelete(garbage []string, target *Backup) []string {
	garbageToDelete := make([]string, 0)
	for _, garbageName := range garbage {
		if strings.HasPrefix(garbageName, backupNamePrefix) && garbageName < target.Name {
			tracelog.InfoLogger.Printf("%v will be deleted (garbage)\n", garbageName)
			garbageToDelete = append(garbageToDelete, garbageName)
		} else {
			tracelog.InfoLogger.Printf("%v skipped (garbage)\n", garbageName)
		}
	}
	return garbageToDelete
}

// ComputeDeletionSkiplineAndPrintIntentions selects last backup and name of last necessary WAL
func ComputeDeletionSkiplineAndPrintIntentions(backups []BackupTime, target *Backup) (skipLine int, walSkipFileName string) {
	skip := true
	skipLine = len(backups)
	walSkipFileName = ""
	for i, backupTime := range backups {
		if skip {
			tracelog.InfoLogger.Printf("%v skipped\n", backupTime.BackupName)
			if walSkipFileName == "" || walSkipFileName > backupTime.WalFileName {
				walSkipFileName = backupTime.WalFileName
			}
		} else {
			tracelog.InfoLogger.Printf("%v will be deleted\n", backupTime.BackupName)
		}
		if backupTime.BackupName == target.Name {
			skip = false
			skipLine = i
		}
	}
	return skipLine, walSkipFileName
}

// TODO : unit tests
func deleteBackupsBefore(backups []BackupTime, skipline int, folder storage.Folder) {
	for i, b := range backups {
		if i > skipline {
			dropBackup(folder, b.BackupName)
		}
	}
}

// TODO : unit tests
func dropBackup(folder storage.Folder, backupName string) {
	basebackupFolder := folder.GetSubFolder(BaseBackupPath)
	backup := NewBackup(basebackupFolder, backupName)
	tarNames, err := backup.GetTarNames()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	sentinelName := backupName + SentinelSuffix
	err = basebackupFolder.DeleteObjects([]string{sentinelName})
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to delete backup sentinel ", sentinelName, err)
	}

	err = basebackupFolder.GetSubFolder(backupName).GetSubFolder(TarPartitionFolderName).DeleteObjects(tarNames)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to delete backup ", backupName, err)
	}

	err = basebackupFolder.GetSubFolder(backupName).DeleteObjects([]string{TarPartitionFolderName})
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to delete backup tar partition folder", backupName, err)
	}

	err = basebackupFolder.DeleteObjects([]string{backupName})
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to delete backup folder", backupName, err)
	}
}

// TODO : unit tests
func DeleteWALBefore(walSkipFileName string, walFolder storage.Folder) {
	wals, err := getWals(walSkipFileName, walFolder)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to obtain WALs for border ", walSkipFileName, err)
	}
	err = walFolder.DeleteObjects(wals)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Unable to delete WALs before '%s', because of: "+tracelog.GetErrorFormatter(), walSkipFileName, err)
	}
}

// TODO : unit tests
// getWals returns all WAL file keys less then key provided
func getWals(before string, folder storage.Folder) ([]string, error) {
	walObjects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	walsBefore := make([]string, 0)
	for _, walObject := range walObjects {
		tracelog.InfoLogger.Println(walObject.GetName())
		if walObject.GetName() < before {
			tracelog.InfoLogger.Println("delete", walObject.GetName())
			walsBefore = append(walsBefore, walObject.GetName())
		}
	}

	return walsBefore, nil
}

// TODO : unit tests
func GetLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := getBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

// TODO : unit tests
// getBackups receives backup descriptions and sorts them by time
func getBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backups, _, err = getBackupsAndGarbage(folder)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

// TODO : unit tests
func getBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(BaseBackupPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := getBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func getBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{StripBackupName(key), time, stripWalFileName(key)}
	}
	slice := TimeSlice(sortTimes)
	sort.Sort(slice)
	return slice
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := stripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func FindTargetBeforeName(folder storage.Folder, name string, modifier int) (storage.Object, error) {
	backup := NewBackup(folder.GetSubFolder(BaseBackupPath), name)
	backup, err := adjustDeleteTarget(backup, modifier == FindFullDeleteModifier)
	if err != nil {
		return nil, err
	}
	objects, _, err := folder.GetSubFolder(BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if strings.HasPrefix(object.GetName(), backup.Name) {
			return object, nil
		}
	}
	return nil, BackupNonExistenceError{}
}
