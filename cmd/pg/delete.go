package pg

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
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
var deleteTargetUserData = ""

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

var deleteTargetCmd = &cobra.Command{
	Use:     internal.DeleteTargetUsageExample, // TODO : improve description
	Example: internal.DeleteTargetExamples,
	Args:    internal.DeleteTargetArgsValidator,
	Run:     runDeleteTarget,
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

func runDeleteTarget(cmd *cobra.Command, args []string) {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	permanentBackups, permanentWals := internal.GetPermanentObjects(folder)
	if len(permanentBackups) > 0 {
		tracelog.InfoLogger.Printf("Found permanent objects: backups=%v, wals=%v\n",
			permanentBackups, permanentWals)
	}

	findFullBackup := false
	modifier := internal.ExtractDeleteTargetModifierFromArgs(args)
	if modifier == internal.FindFullDeleteModifier {
		findFullBackup = true
		// remove the extracted modifier from args
		args = args[1:]
	}

	deleteHandler, err := newPostgresDeleteHandler(folder, permanentBackups, permanentWals)
	tracelog.ErrorLogger.FatalOnError(err)
	targetBackupSelector, err := createTargetDeleteBackupSelector(cmd, args, deleteTargetUserData)
	tracelog.ErrorLogger.FatalOnError(err)
	deleteHandler.HandleDeleteTarget(targetBackupSelector, confirmed, findFullBackup)
}

func init() {
	cmd.AddCommand(deleteCmd)

	deleteTargetCmd.Flags().StringVar(
		&deleteTargetUserData, internal.DeleteTargetUserDataFlag, "", internal.DeleteTargetUserDataDescription)

	deleteCmd.AddCommand(deleteRetainCmd, deleteBeforeCmd, deleteEverythingCmd, deleteTargetCmd)
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
	postgresBackups, err := makePostgresBackupObjects(folder, backups, startTimeByBackupName)
	if err != nil {
		return nil, err
	}

	deleteHandler := internal.NewDeleteHandler(
		folder,
		postgresBackups,
		lessFunc,
		internal.IsPermanentFunc(
			makePostgresPermanentFunc(permanentBackups, permanentWals)),
	)

	return deleteHandler, nil
}

func newPostgresBackupObject(incrementBase, incrementFrom string,
	isFullBackup bool, creationTime time.Time, object storage.Object) PostgresBackupObject {
	return PostgresBackupObject{
		Object:            object,
		isFullBackup:      isFullBackup,
		baseBackupName:    incrementBase,
		incrementFromName: incrementFrom,
		creationTime:      creationTime,
		BackupName:        internal.FetchPgBackupName(object),
	}
}

type PostgresBackupObject struct {
	storage.Object
	BackupName        string
	isFullBackup      bool
	baseBackupName    string
	incrementFromName string
	creationTime      time.Time
}

func (o PostgresBackupObject) IsFullBackup() bool {
	return o.isFullBackup
}

func (o PostgresBackupObject) GetBaseBackupName() string {
	return o.baseBackupName
}

func (o PostgresBackupObject) GetBackupTime() time.Time {
	return o.creationTime
}

func (o PostgresBackupObject) GetBackupName() string {
	return o.BackupName
}

func (o PostgresBackupObject) GetIncrementFromName() string {
	return o.incrementFromName
}

func makePostgresBackupObjects(
	folder storage.Folder, objects []storage.Object, startTimeByBackupName map[string]time.Time,
) ([]internal.BackupObject, error) {
	backupObjects := make([]internal.BackupObject, 0, len(objects))
	for _, object := range objects {
		incrementBase, incrementFrom, isFullBackup, err := postgresGetIncrementInfo(folder, object)
		if err != nil {
			return nil, err
		}
		postgresBackup := newPostgresBackupObject(
			incrementBase, incrementFrom, isFullBackup, object.GetLastModified(), object)

		if startTimeByBackupName != nil {
			postgresBackup.creationTime = startTimeByBackupName[postgresBackup.BackupName]
		}
		backupObjects = append(backupObjects, postgresBackup)
	}
	return backupObjects, nil
}

func makePostgresPermanentFunc(permanentBackups, permanentWals map[string]bool) func(object storage.Object) bool {
	return func(object storage.Object) bool {
		return internal.IsPermanent(object.GetName(), permanentBackups, permanentWals)
	}
}

func makeLessFunc(startTimeByBackupName map[string]time.Time) func(storage.Object, storage.Object) bool {
	return func(object1 storage.Object, object2 storage.Object) bool {
		backupName1 := internal.FetchPgBackupName(object1)
		if backupName1 == "" {
			// we can't compare non-backup storage objects (probably WAL segments) by start time,
			// so use the segment number comparator instead
			return postgresSegmentNoLess(object1, object2)
		}
		backupName2 := internal.FetchPgBackupName(object2)
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

func postgresGetIncrementInfo(folder storage.Folder, object storage.Object) (string, string, bool, error) {
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), internal.FetchPgBackupName(object))
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return "", "", true, err
	}
	if !sentinel.IsIncremental() {
		return "", "", true, nil
	}

	return *sentinel.IncrementFullName, *sentinel.IncrementFrom, false, nil
}

// create the BackupSelector to select the backup to delete
func createTargetDeleteBackupSelector(cmd *cobra.Command,
	args []string, targetUserData string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) > 0 {
		targetName = args[0]
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName)
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}
