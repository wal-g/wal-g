package walg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/wal-g/wal-g/tracelog"
	"log"
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
	//if DeleteConfirmed && !DeleteDryrun  // TODO: use flag
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
func deleteBeforeTarget(target *Backup, findFull bool, backups []BackupTime, dryRun bool) {
	folder := target.Folder
	dto := target.fetchSentinel()
	if dto.isIncremental() {
		if findFull {
			target.Name = *dto.IncrementFullName
		} else {
			tracelog.ErrorLogger.Fatalf("%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option.", target.Name)
		}
	}
	var garbage []BackupTime
	var err error
	if backups == nil {
		backups, garbage, err = getBackupsAndGarbage(folder)
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
	}

	skipLine, walSkipFileName := ComputeDeletionSkipline(backups, target)

	for _, backupTime := range garbage {
		if strings.HasPrefix(backupTime.Name, backupNamePrefix) && backupTime.Name < target.Name {
			tracelog.InfoLogger.Printf("%v will be deleted (garbage)\n", backupTime.Name)
			if !dryRun {
				dropBackup(folder, backupTime)
			}
		} else {
			tracelog.InfoLogger.Printf("%v skipped (garbage)\n", backupTime.Name)
		}
	}

	if !dryRun {
		if skipLine < len(backups)-1 {
			deleteWALBefore(walSkipFileName, folder)
			deleteBackupsBefore(backups, skipLine, folder)
		}
	} else {
		tracelog.InfoLogger.Printf("Dry run finished.\n")
	}
}

// ComputeDeletionSkipline selects last backup and name of last necessary WAL
func ComputeDeletionSkipline(backups []BackupTime, target *Backup) (skipLine int, walSkipFileName string) {
	skip := true
	skipLine = len(backups)
	walSkipFileName = ""
	for i, backupTime := range backups {
		if skip {
			tracelog.InfoLogger.Printf("%v skipped\n", backupTime.Name)
			if walSkipFileName == "" || walSkipFileName > backupTime.WalFileName {
				walSkipFileName = backupTime.WalFileName
			}
		} else {
			tracelog.InfoLogger.Printf("%v will be deleted\n", backupTime.Name)
		}
		if backupTime.Name == target.Name {
			skip = false
			skipLine = i
		}
	}
	return skipLine, walSkipFileName
}

// TODO : unit tests
func deleteBackupsBefore(backups []BackupTime, skipline int, folder *S3Folder) {
	for i, b := range backups {
		if i > skipline {
			dropBackup(folder, b)
		}
	}
}

// TODO : unit tests
func dropBackup(folder *S3Folder, backupTime BackupTime) {
	backup := NewBackup(folder, backupTime.Name)
	tarFiles, err := backup.GetKeys()
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to list backup for deletion ", backupTime.Name, err)
	}

	folderKey := strings.TrimPrefix(GetBackupPath(folder)+backupTime.Name, "/")
	sentinelKey := folderKey + SentinelSuffix

	keys := append(tarFiles, sentinelKey, folderKey)
	parts := partition(keys, 1000)
	for _, part := range parts {

		input := &s3.DeleteObjectsInput{Bucket: folder.Bucket, Delete: &s3.Delete{
			Objects: partitionToObjects(part),
		}}
		_, err = folder.S3API.DeleteObjects(input)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Unable to delete backup ", backupTime.Name, err)
		}

	}
}

// TODO : unit tests
func partitionToObjects(keys []string) []*s3.ObjectIdentifier {
	objs := make([]*s3.ObjectIdentifier, len(keys))
	for i, k := range keys {
		objs[i] = &s3.ObjectIdentifier{Key: aws.String(k)}
	}
	return objs
}

// TODO : unit tests
func deleteWALBefore(walSkipFileName string, folder *S3Folder) {
	objects, err := getWals(walSkipFileName, folder)
	if err != nil {
		tracelog.ErrorLogger.Fatal("Unable to obtaind WALS for border ", walSkipFileName, err)
	}
	parts := partitionObjects(objects, 1000)
	for _, part := range parts {
		input := &s3.DeleteObjectsInput{Bucket: folder.Bucket, Delete: &s3.Delete{
			Objects: part,
		}}
		_, err = folder.S3API.DeleteObjects(input)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Unable to delete WALS before ", walSkipFileName, err)
		}
	}
}

func printDeleteUsageAndFail() {
	log.Fatal(DeleteUsageText)
}
