package internal

import (
	"fmt"
	"github.com/wal-g/tracelog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	NoDeleteModifier = iota
	FullDeleteModifier
	FindFullDeleteModifier
	ForceDeleteModifier
	ConfirmFlag            = "confirm"
	DeleteShortDescription = "Clears old backups and WALs"

	DeleteRetainExamples = `  retain 5                      keep 5 backups
  retain FULL 5                 keep 5 full backups and all deltas of them
  retain FIND_FULL 5            find necessary full for 5th and keep everything after it
  retain 5 --after 2019-12-12T12:12:12   keep 5 most recent backups and backups made after 2019-12-12 12:12:12`

	DeleteBeforeExamples = `  before base_0123              keep everything after base_0123 including itself
  before FIND_FULL base_0123    keep everything after the base of base_0123`

	DeleteEverythingExamples = `  everything                delete every backup only if there is no permanent backups
  everything FORCE          delete every backup include permanents`

	DeleteEverythingUsageExample = "everything [FORCE]"
	DeleteRetainUsageExample     = "retain [FULL|FIND_FULL] backup_count"
	DeleteBeforeUsageExample     = "before [FIND_FULL] backup_name|timestamp"
)

var StringModifiers = []string{"FULL", "FIND_FULL"}
var StringModifiersDeleteEverything = []string{"FORCE"}
var MaxTime = time.Unix(1<<63-62135596801, 999999999)

// BackupObject represents
// the backup sentinel object uploaded on storage
type BackupObject interface {
	storage.Object
	IsFullBackup() bool
	GetBackupTime() time.Time
}

func NewDeleteHandler(
	folder storage.Folder,
	backups []BackupObject,
	less func(object1, object2 storage.Object) bool,
	isPermanent func(object storage.Object) bool,
) *DeleteHandler {
	return &DeleteHandler{
		folder:  folder,
		backups: backups,
		less:    less,
		greater: func(object1, object2 storage.Object) bool {
			return less(object2, object1)
		},
		isPermanent: isPermanent,
	}
}

type DeleteHandler struct {
	folder  storage.Folder
	backups []BackupObject

	less    func(object1, object2 storage.Object) bool
	greater func(object1, object2 storage.Object) bool

	isPermanent func(object storage.Object) bool
}

func (h *DeleteHandler) HandleDeleteBefore(args []string, confirmed bool) {
	modifier, beforeStr := extractDeleteModifierFromArgs(args)

	var target BackupObject
	timeLine, err := time.Parse(time.RFC3339, beforeStr)
	if err == nil {
		target, err = h.FindTargetBeforeTime(timeLine, modifier)
	} else {
		target, err = h.FindTargetBeforeName(beforeStr, modifier)
	}

	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(args []string, confirmed bool) {
	modifier, retentionStr := extractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}
	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetainAfter(args []string, confirmed bool) {

	modifier, retentionSir, afterStr := extractDeleteRetainModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	var target BackupObject
	timeLine, err := time.Parse(time.RFC3339, afterStr)
	if err == nil {
		target, err = h.FindTargetRetainAfterTime(retentionCount, timeLine, modifier)
	} else {
		target, err = h.FindTargetRetainAfterName(retentionCount, afterStr, modifier)
	}
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) FindTargetBeforeName(name string, modifier int) (BackupObject, error) {
	choiceFunc := getBeforeChoiceFunc(name, modifier)
	if choiceFunc == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete before'")
	}
	return findTarget(h.backups, h.greater, choiceFunc)
}

func (h *DeleteHandler) FindTargetBeforeTime(timeLine time.Time, modifier int) (BackupObject, error) {
	potentialTarget, err := findTarget(h.backups, h.less, func(object BackupObject) bool {
		backupTime := object.GetBackupTime()
		return timeLine.Before(backupTime) || timeLine.Equal(backupTime)
	})
	if err != nil {
		return nil, err
	}
	if potentialTarget == nil {
		return nil, nil
	}

	return h.FindTargetBeforeName(potentialTarget.GetName(), modifier)
}

func (h *DeleteHandler) FindTargetRetain(retentionCount, modifier int) (BackupObject, error) {
	choiceFunc := getRetainChoiceFunc(retentionCount, modifier)
	if choiceFunc == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete retain'")
	}
	return findTarget(h.backups, h.greater, choiceFunc)
}

func (h *DeleteHandler) FindTargetRetainAfterName(
	retentionCount int, name string, modifier int) (BackupObject, error) {

	choiceFuncRetain := getRetainChoiceFunc(retentionCount, modifier)
	if choiceFuncRetain == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete before'")
	}
	meetName := false
	choiceFuncAfterName := func(object BackupObject) bool {
		meetName = meetName || strings.HasPrefix(object.GetName(), name)
		if modifier == NoDeleteModifier {
			return meetName
		} else {
			return meetName && object.IsFullBackup()
		}
	}
	if choiceFuncAfterName == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete before'")
	}

	target1, err := findTarget(h.backups, h.greater, choiceFuncRetain)
	if err != nil {
		return nil, err
	}
	target2, err := findTarget(h.backups, h.less, choiceFuncAfterName)
	if err != nil {
		return nil, err
	}

	if h.greater(target2, target1) {
		return target1, nil
	} else {
		return target2, nil
	}
}

func (h *DeleteHandler) FindTargetRetainAfterTime(retentionCount int, timeLine time.Time, modifier int,
) (BackupObject, error) {

	choiceFuncRetain := getRetainChoiceFunc(retentionCount, modifier)
	if choiceFuncRetain == nil {
		return nil, utility.NewForbiddenActionError("Not allowed modifier for 'delete retain'")
	}
	choiceFuncAfter := func(object BackupObject) bool {
		backupTime := object.GetBackupTime()
		timeCheck := timeLine.Before(backupTime) || timeLine.Equal(backupTime)
		if modifier == NoDeleteModifier {
			return timeCheck
		} else {
			return timeCheck && object.IsFullBackup()
		}
	}

	target1, err := findTarget(h.backups, h.greater, choiceFuncRetain)
	if err != nil {
		return nil, err
	}
	target2, err := findTarget(h.backups, h.less, choiceFuncAfter)
	if err != nil {
		return nil, err
	}

	if target1 == nil {
		return target2, nil
	}
	if target2 == nil {
		return target1, nil
	}

	if h.greater(target2, target1) {
		return target1, nil
	}

	return target2, nil
}

func (h *DeleteHandler) DeleteEverything(confirmed bool) {
	filter := func(object storage.Object) bool { return true }
	err := storage.DeleteObjectsWhere(h.folder, confirmed, filter)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) DeleteBeforeTarget(target BackupObject, confirmed bool) error {

	if !target.IsFullBackup() {
		errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
		return utility.NewForbiddenActionError(fmt.Sprintf(errorMessage, target.GetName()))
	}
	tracelog.InfoLogger.Println("Start delete")

	return storage.DeleteObjectsWhere(h.folder, confirmed, func(object storage.Object) bool {
		return h.less(object, target) && !h.isPermanent(object)
	})
}

func findTarget(objects []BackupObject,
	compare func(object1, object2 storage.Object) bool,
	isTarget func(object BackupObject) bool) (BackupObject, error) {

	sort.Slice(objects, func(i, j int) bool {
		return compare(objects[i], objects[j])
	})
	for _, object := range objects {
		if isTarget(object) {
			return object, nil
		}
	}
	return nil, nil
}

func getBeforeChoiceFunc(name string, modifier int) func(object BackupObject) bool {
	meetName := false
	switch modifier {
	case NoDeleteModifier:
		return func(object BackupObject) bool {
			return strings.HasPrefix(object.GetName(), name)
		}
	case FindFullDeleteModifier:
		return func(object BackupObject) bool {
			meetName = meetName || strings.HasPrefix(object.GetName(), name)
			return meetName && object.IsFullBackup()
		}
	}
	return nil
}

func getRetainChoiceFunc(retentionCount, modifier int) func(object BackupObject) bool {

	count := 0
	switch modifier {
	case NoDeleteModifier:
		return func(object BackupObject) bool {
			count++
			if count == retentionCount {
				return true
			}
			return false
		}
	case FullDeleteModifier:
		return func(object BackupObject) bool {
			if object.IsFullBackup() {
				count++
			}
			if count == retentionCount {
				return true
			}
			return false
		}
	case FindFullDeleteModifier:
		return func(object BackupObject) bool {
			count++
			if count >= retentionCount && object.IsFullBackup() {
				return true
			}
			return false
		}
	}
	return nil
}

// TODO : unit tests
func getLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := GetBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

func GetBackupSentinelObjects(folder storage.Folder) ([]storage.Object, error) {
	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sentinelObjects := make([]storage.Object, 0, len(objects))
	for _, object := range objects {
		if !strings.HasSuffix(object.GetName(), utility.SentinelSuffix) {
			continue
		}
		sentinelObjects = append(sentinelObjects, object)
	}

	return sentinelObjects, nil
}

// TODO : unit tests
// GetBackups receives backup descriptions and sorts them by time
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	backups, _, err = GetBackupsAndGarbage(folder)
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
func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func GetBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{utility.StripBackupName(key), time,
			utility.StripWalFileName(key)}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := utility.StripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func extractDeleteRetainModifierFromArgs(args []string) (int, string, string) {
	if len(args) == 2 {
		return NoDeleteModifier, args[0], args[1]
	} else if args[0] == StringModifiers[0] {
		return FullDeleteModifier, args[1], args[2]
	}
	return FindFullDeleteModifier, args[1], args[2]
}

func ExtractDeleteEverythingModifierFromArgs(args []string) int {
	if len(args) == 0 {
		return NoDeleteModifier
	}
	return ForceDeleteModifier
}

func extractDeleteModifierFromArgs(args []string) (int, string) {
	if len(args) == 1 {
		return NoDeleteModifier, args[0]
	} else if args[0] == StringModifiers[0] {
		return FullDeleteModifier, args[1]
	}
	return FindFullDeleteModifier, args[1]
}

func DeleteBeforeArgsValidator(cmd *cobra.Command, args []string) error {
	err := deleteArgsValidator(cmd, args, StringModifiers, 1, 2)
	if err != nil {
		return err
	}
	modifier, beforeStr := extractDeleteModifierFromArgs(args)
	if modifier == FullDeleteModifier {
		return fmt.Errorf("unsupported moodifier for delete before command")
	}
	if before, err := time.Parse(time.RFC3339, beforeStr); err == nil {
		if before.After(utility.TimeNowCrossPlatformUTC()) {
			return fmt.Errorf("cannot delete before future date")
		}
	}
	return nil
}

func DeleteEverythingArgsValidator(cmd *cobra.Command, args []string) error {
	return deleteArgsValidator(cmd, args, StringModifiersDeleteEverything, 0, 1)
}

func DeleteRetainArgsValidator(cmd *cobra.Command, args []string) error {
	_, retentionStr := extractDeleteModifierFromArgs(args)
	retentionNumber, err := strconv.Atoi(retentionStr)
	if err != nil {
		return errors.Wrapf(err, "expected to get a number as retantion count, but got: '%s'", retentionStr)
	}
	if retentionNumber <= 0 {
		return fmt.Errorf("cannot retain less than one backup. Check out delete everything")
	}
	return nil
}

func DeleteRetainAfterArgsValidator(cmd *cobra.Command, args []string) error {
	err := deleteArgsValidator(cmd, args, StringModifiers, 2, 3)
	if err != nil {
		return err
	}
	_, retentionStr, afterStr := extractDeleteRetainModifierFromArgs(args)
	retentionNumber, err := strconv.Atoi(retentionStr)
	if err != nil {
		return errors.Wrapf(err, "expected to get a number as retantion count, but got: '%s'", retentionStr)
	}
	if retentionNumber <= 0 {
		return fmt.Errorf("cannot retain less than one backup. Check out delete everything")
	}
	if before, err := time.Parse(time.RFC3339, afterStr); err == nil {
		if before.After(utility.TimeNowCrossPlatformUTC()) {
			return fmt.Errorf("cannot delete retain future date")
		}
	}
	return nil
}

func deleteArgsValidator(cmd *cobra.Command, args, stringModifiers []string, minArgs int, maxArgs int) error {
	if len(args) < minArgs || len(args) > maxArgs {
		return fmt.Errorf("accepts between %d and %d arg(s), received %d", minArgs, maxArgs, len(args))
	}
	if len(args) == maxArgs {
		expectedModifier := args[0]
		isModifierInList := false
		for _, modifier := range stringModifiers {
			if isModifierInList = modifier == expectedModifier; isModifierInList {
				break
			}
		}
		if !isModifierInList {
			return fmt.Errorf("expected to get one of modifiers: %v as first argument", stringModifiers)
		}
	}
	return nil
}
