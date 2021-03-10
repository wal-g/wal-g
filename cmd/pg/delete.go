package pg

import (
	"fmt"
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

	permanentBackups, permanentWals := internal.GetPermanentObjects(folder)
	if len(permanentBackups) > 0 {
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n",
			permanentBackups, permanentWals)
	}

	deleteHandler, err := newPostgresDeleteHandler(folder, permanentBackups, permanentWals)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteBefore(args, confirmed)
}

func runDeleteRetain(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := internal.GetPermanentObjects(folder)
	if len(permanentBackups) > 0 {
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n",
			permanentBackups, permanentWals)
	}

	deleteHandler, err := newPostgresDeleteHandler(folder, permanentBackups, permanentWals)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.HandleDeleteRetain(args, confirmed)
}

func runDeleteEverything(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	forceModifier := false
	modifier := internal.ExtractDeleteEverythingModifierFromArgs(args)
	if modifier == internal.ForceDeleteModifier {
		forceModifier = true
	}

	permanentBackups, permanentWals := internal.GetPermanentObjects(folder)
	if len(permanentBackups) > 0 {
		if !forceModifier {
			tracelog.ErrorLogger.Fatalf("Found permanent objects: backups=%v, wals=%v\n",
				permanentBackups, permanentWals)
		}
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n",
			permanentBackups, permanentWals)
	}

	deleteHandler, err := newPostgresDeleteHandler(folder, permanentBackups, permanentWals)
	tracelog.ErrorLogger.FatalOnError(err)

	deleteHandler.DeleteEverything(confirmed)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd)
	deleteCmd.PersistentFlags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.PersistentFlags().BoolVar(&useSentinelTime, UseSentinelTimeFlag, false, UseSentinelTimeDescription)
}

func newPostgresDeleteHandler(folder storage.Folder, permanentBackups, permanentWals map[string]bool,
) (*internal.DeleteHandler, error) {
	backups, err := internal.GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	lessFunc := postgresTimelineAndSegmentNoLess
	var startTimeByBackupName map[string]time.Time
	if useSentinelTime {
		// If all backups in storage have metadata, we will use backup start time from sentinel.
		// Otherwise, for example in case when we are dealing with some ancient backup without
		// metadata included, fall back to the default timeline and segment number comparator.
		startTimeByBackupName, err = getBackupStartTimeMap(folder, backups)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get sentinel backup start times: %v,"+
				" will fall back to timeline and segment number for ordering...\n", err)
		} else {
			lessFunc = makeLessFunc(startTimeByBackupName)
		}
	}
	postgresBackups := makePostgresBackupObjects(folder, backups, startTimeByBackupName)

	deleteHandler := internal.NewDeleteHandler(
		folder,
		postgresBackups,
		lessFunc,
		internal.IsPermanentFunc(
			makePostgresPermanentFunc(permanentBackups, permanentWals)),
	)

	return deleteHandler, nil
}

func newPostgresBackupObject(isFullBackup bool, creationTime time.Time, object storage.Object) PostgresBackupObject {
	return PostgresBackupObject{
		Object:       object,
		isFullBackup: isFullBackup,
		creationTime: creationTime,
	}
}

type PostgresBackupObject struct {
	storage.Object
	isFullBackup bool
	creationTime time.Time
}

func (o PostgresBackupObject) IsFullBackup() bool {
	return o.isFullBackup
}

func (o PostgresBackupObject) GetBackupTime() time.Time {
	return o.creationTime
}

func makePostgresBackupObjects(folder storage.Folder, objects []storage.Object, startTimeByBackupName map[string]time.Time) []internal.BackupObject {
	backupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		postgresBackup := newPostgresBackupObject(
			postgresIsFullBackup(folder, object), object.GetLastModified(), object)

		if startTimeByBackupName != nil {
			backupName := fetchBackupName(object)
			postgresBackup.creationTime = startTimeByBackupName[backupName]
		}
		backupObjects = append(backupObjects, postgresBackup)
	}
	return backupObjects
}

func makePostgresPermanentFunc(permanentBackups, permanentWals map[string]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		return internal.IsPermanent(object.GetName(), permanentBackups, permanentWals)
	}
}

func makeLessFunc(startTimeByBackupName map[string]time.Time) func(storage.Object, storage.Object) bool {
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

// getBackupStartTimeMap returns a map for a fast lookup of the backup start time by the backup name
func getBackupStartTimeMap(folder storage.Folder, backups []storage.Object) (map[string]time.Time, error) {
	backupTimes, err := internal.GetBackupTimeSlices(backups, folder, internal.CreationTime)
	if err != nil {
		return nil, err
	}
	startTimeByBackupName := make(map[string]time.Time, len(backups))
	for _, backupTime := range backupTimes {
		startTimeByBackupName[backupTime.BackupName] = backupTime.Time
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
