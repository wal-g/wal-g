package pg

import (
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const UseSentinelTimeFlag = "use-sentinel-time"
const UseSentinelTimeDescription = "Use backup creation time from sentinel for backups ordering."

var confirmed = false
var useSentinelTime = false
var patternBackupName = fmt.Sprintf("base_%[1]s(_D_%[1]s)?", internal.PatternTimelineAndLogSegNo)
var regexpBackupName = regexp.MustCompile(patternBackupName)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: internal.DeleteShortDescription, // TODO : improve description
}

var deleteBeforeCmd = &cobra.Command{
	Use:     internal.DeleteBeforeUsageExample, // TODO : improve description
	Example: internal.DeleteBeforeExamples,
	Args:    internal.DeleteBeforeArgsValidator,
	Run:     runDeleteBefore,
}

var deleteRetainCmd = &cobra.Command{
	Use:       internal.DeleteRetainUsageExample, // TODO : improve description
	Example:   internal.DeleteRetainExamples,
	ValidArgs: internal.StringModifiers,
	Run:       runDeleteRetain,
}

var deleteEverythingCmd = &cobra.Command{
	Use:       internal.DeleteEverythingUsageExample, // TODO : improve description
	Example:   internal.DeleteEverythingExamples,
	ValidArgs: internal.StringModifiersDeleteEverything,
	Args:      internal.DeleteEverythingArgsValidator,
	Run:       runDeleteEverything,
}

func runDeleteBefore(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}

	backups, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	lessFunc := postgresTimelineAndSegmentNoLess
	backupTimeFunc := getBackupTime
	if newLessFunc, newTimeFunc, ok := tryMakeSentinelTimeFuncs(folder, backups); ok {
		lessFunc, backupTimeFunc = newLessFunc, newTimeFunc
	}

	internal.HandleDeleteBefore(folder, backups, args, confirmed, isFullBackup, lessFunc, backupTimeFunc)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	isFullBackup := func(object storage.Object) bool {
		return postgresIsFullBackup(folder, object)
	}
	backups, err := internal.GetBackupSentinelObjects(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	lessFunc := postgresTimelineAndSegmentNoLess
	if newLessFunc, _, ok := tryMakeSentinelTimeFuncs(folder, backups); ok {
		lessFunc = newLessFunc
	}

	internal.HandleDeleteRetain(folder, backups, args, confirmed, isFullBackup, lessFunc)
}

// tryMakeSentinelTimeFuncs tries to create sentinel time based functions for delete handler
func tryMakeSentinelTimeFuncs(
	folder storage.Folder,
	backups []storage.Object,
) (func(obj1, obj2 storage.Object) bool, func(obj storage.Object) time.Time, bool) {
	if !useSentinelTime {
		return nil, nil, false
	}

	// If all backups in storage have metadata, we will use backup start time from sentinel.
	// Otherwise, for example in case when we are dealing with some ancient backup without
	// metadata included, fall back to the default timeline and segment number comparator.
	startTimeByBackupName, err := getBackupStartTimeMap(folder, backups)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to get sentinel backup start times: %v,"+
			" will fall back to timeline and segment number for ordering...\n", err)
		return nil, nil, false
	}

	return lessFunc(startTimeByBackupName), GetBackupTimeFunc(startTimeByBackupName), true
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)
	internal.DeleteEverything(folder, confirmed, args)
}

func init() {
	Cmd.AddCommand(deleteCmd)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.PersistentFlags().BoolVar(&useSentinelTime, UseSentinelTimeFlag, false, UseSentinelTimeDescription)
}

func lessFunc(startTimeByBackupName map[string]time.Time) func(storage.Object, storage.Object) bool {
	return func(object1 storage.Object, object2 storage.Object) bool {
		backupName1 := fetchBackupName(object1)
		if backupName1 == "" {
			// we can't compare non-backup storage objects (probably WAL segments) by start time,
			// so use the segment number comparator instead
			return postgresSegmentNoLess(object1, object2)
		}
		backupName2 := fetchBackupName(object2)
		if backupName2 == "" {
			return postgresSegmentNoLess(object1, object2)
		}
		startTime1, ok := startTimeByBackupName[backupName1]
		if !ok {
			return false
		}
		startTime2, ok := startTimeByBackupName[backupName2]
		if !ok {
			return false
		}
		return startTime1.Before(startTime2)
	}
}

func GetBackupTimeFunc(startTimeByBackupName map[string]time.Time) func(storage.Object) time.Time {
	return func(backupObject storage.Object) time.Time {
		backupName := fetchBackupName(backupObject)
		return startTimeByBackupName[backupName]
	}
}

// getBackupStartTimeMap returns a map for a fast lookup of the backup start time by the backup name
func getBackupStartTimeMap(folder storage.Folder, backups []storage.Object) (map[string]time.Time, error) {
	backupTimes := internal.GetBackupTimeSlices(backups)
	startTimeByBackupName := make(map[string]time.Time, len(backups))

	for _, backupTime := range backupTimes {
		backupDetails, err := internal.GetBackupDetails(folder, backupTime)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to get metadata of backup %s",
				backupTime.BackupName)
		}
		startTimeByBackupName[backupDetails.BackupName] = backupDetails.StartTime
	}
	return startTimeByBackupName, nil
}

// TODO: create postgres part and move it there, if it will be needed
func postgresSegmentNoLess(object1 storage.Object, object2 storage.Object) bool {
	_, segmentNumber1, ok := internal.TryFetchTimelineAndLogSegNo(object1.GetName())
	if !ok {
		return false
	}
	_, segmentNumber2, ok := internal.TryFetchTimelineAndLogSegNo(object2.GetName())
	if !ok {
		return false
	}
	return segmentNumber1 < segmentNumber2
}

// TODO: create postgres part and move it there, if it will be needed
func postgresTimelineAndSegmentNoLess(object1 storage.Object, object2 storage.Object) bool {
	tl1, segNo1, ok := internal.TryFetchTimelineAndLogSegNo(object1.GetName())
	if !ok {
		return false
	}
	tl2, segNo2, ok := internal.TryFetchTimelineAndLogSegNo(object2.GetName())
	if !ok {
		return false
	}
	return tl1 < tl2 || tl1 == tl2 && segNo1 < segNo2
}

func postgresIsFullBackup(folder storage.Folder, object storage.Object) bool {
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), fetchBackupName(object))
	sentinel, _ := backup.GetSentinel()
	return !sentinel.IsIncremental()
}

func fetchBackupName(object storage.Object) string {
	return regexpBackupName.FindString(object.GetName())
}

func getBackupTime(backupObject storage.Object) time.Time {
	return backupObject.GetLastModified()
}
