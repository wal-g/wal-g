package internal

import (
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

const DeleteUsageText = "delete requires at least 2 parameters" + `
		retain 5                      keep 5 backups
		retain FULL 5                 keep 5 full backups and all deltas of them
		retain FIND_FULL 5            find necessary full for 5th and keep everything after it
		before base_0123              keep everything after base_0123 including itself
		before FIND_FULL base_0123    keep everything after the base of base_0123`

// DeleteCommandArguments incapsulates arguments for delete command
type DeleteCommandArguments struct {
	Full       bool
	FindFull   bool
	Retain     bool
	Before     bool
	Target     string
	BeforeTime *time.Time
	dryrun     bool
}

// TODO : unit tests
func adjustDeleteTarget(target *Backup, findFull bool) *Backup {
	sentinelDto, err := target.FetchSentinel()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	if sentinelDto.isIncremental() {
		if findFull {
			target.Name = *sentinelDto.IncrementFullName
		} else {
			tracelog.ErrorLogger.Fatalf("%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option.", target.Name)
		}
	}
	return target
}

// TODO : unit tests
// HandleDelete is invoked to perform wal-g delete
func HandleDelete(folder storage.Folder, args []string) {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)

	arguments := ParseDeleteArguments(args, printDeleteUsageAndFail)

	if arguments.Before {
		if arguments.BeforeTime == nil {
			deleteBeforeTarget(folder, adjustDeleteTarget(NewBackup(baseBackupFolder, arguments.Target), arguments.FindFull), arguments.dryrun)
		} else {
			backups, err := getBackups(folder)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			for _, b := range backups {
				if b.Time.Before(*arguments.BeforeTime) {
					deleteBeforeTarget(folder, adjustDeleteTarget(NewBackup(baseBackupFolder, b.BackupName), arguments.FindFull), arguments.dryrun)
					return
				}
			}
			tracelog.WarningLogger.Println("No backups before ", *arguments.BeforeTime)
		}
	}
	if arguments.Retain {
		backupCount, err := strconv.Atoi(arguments.Target)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Unable to parse number of backups: ", err)
		}
		backups, err := getBackups(folder)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		if arguments.Full {
			if len(backups) <= backupCount {
				tracelog.WarningLogger.Printf("Have only %v backups.\n", backupCount)
			}
			left := backupCount
			for _, b := range backups {
				if left == 1 {
					deleteBeforeTarget(folder, adjustDeleteTarget(NewBackup(baseBackupFolder, b.BackupName), true), arguments.dryrun)
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
			tracelog.WarningLogger.Printf("Scanned all backups but didn't have %v full.", backupCount)
		} else {
			if len(backups) <= backupCount {
				tracelog.WarningLogger.Printf("Have only %v backups.\n", backupCount)
			} else {
				deleteBeforeTarget(folder, adjustDeleteTarget(NewBackup(baseBackupFolder, backups[backupCount-1].BackupName), arguments.FindFull), arguments.dryrun)
			}
		}
	}
}

// ParseDeleteArguments interprets arguments for delete command. TODO: use flags or cobra
func ParseDeleteArguments(args []string, fallBackFunc func()) (result DeleteCommandArguments) {
	if len(args) < 3 {
		fallBackFunc()
		return
	}

	params := args[1:]
	if params[0] == "retain" {
		result.Retain = true
		params = params[1:]
	} else if params[0] == "before" {
		result.Before = true
		params = params[1:]
	} else {
		fallBackFunc()
		return
	}
	if params[0] == "FULL" {
		result.Full = true
		params = params[1:]
	} else if params[0] == "FIND_FULL" {
		result.FindFull = true
		params = params[1:]
	}
	if len(params) < 1 {
		tracelog.ErrorLogger.Print("Backup name not specified")
		fallBackFunc()
		return
	}

	result.Target = params[0]
	if t, err := time.Parse(time.RFC3339, result.Target); err == nil {
		if t.After(time.Now()) {
			tracelog.WarningLogger.Println("Cannot delete before future date")
			fallBackFunc()
		}
		result.BeforeTime = &t
	}
	// if DeleteConfirmed && !DeleteDryrun  // TODO: use flag
	result.dryrun = true
	if len(params) > 1 && (params[1] == "--confirm" || params[1] == "-confirm") {
		result.dryrun = false
	}

	if result.Retain {
		number, err := strconv.Atoi(result.Target)
		if err != nil {
			tracelog.ErrorLogger.Println("Cannot parse target number ", number)
			fallBackFunc()
			return
		}
		if number <= 0 {
			tracelog.ErrorLogger.Println("Cannot retain 0") // Consider allowing to delete everything
			fallBackFunc()
			return
		}
	}
	return
}

// TODO : unit tests
func deleteBeforeTarget(folder storage.Folder, target *Backup, dryRun bool) {
	walFolder := folder.GetSubFolder(WalPath)
	backupToScan, garbage, err := getBackupsAndGarbage(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	garbageToDelete := findGarbageToDelete(garbage, target)

	skipLine, walSkipFileName := ComputeDeletionSkiplineAndPrintIntentions(backupToScan, target)

	if dryRun { // TODO : split this function by two: 'find objects to delete' and 'delete these objects'
		tracelog.InfoLogger.Printf("Dry run finished.\n")
		return
	}

	for _, garbageName := range garbageToDelete {
		dropBackup(folder, garbageName)
	}
	if skipLine < len(backupToScan)-1 {
		deleteWALBefore(walSkipFileName, walFolder)
		deleteBackupsBefore(backupToScan, skipLine, folder)
	}
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
func deleteWALBefore(walSkipFileName string, walFolder storage.Folder) {
	wals, err := getWals(walSkipFileName, walFolder)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to obtain WALs for border ", walSkipFileName, err)
	}
	err = walFolder.DeleteObjects(wals)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Unable to delete WALs before '%s', because of: "+tracelog.GetErrorFormatter(), walSkipFileName, err)
	}
}

func printDeleteUsageAndFail() {
	log.Fatal(DeleteUsageText)
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
func getLatestBackupName(folder storage.Folder) (string, error) {
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
		sortTimes[i] = BackupTime{stripBackupName(key), time, stripWalFileName(key)}
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
