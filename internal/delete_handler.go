package internal

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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

	DeleteEverythingExamples = `  everything                
	delete every backup only if there is no permanent backups
  everything FORCE          delete every backup include permanents`

	DeleteTargetExamples = `  target base_0000000100000000000000C4	delete base backup by name
  target --target-user-data "{ \"x\": [3], \"y\": 4 }"	delete backup specified by user data
  target base_0000000100000000000000C9_D_0000000100000000000000C4	delete delta backup and all dependant delta backups 
  target FIND_FULL base_0000000100000000000000C9_D_0000000100000000000000C4	delete delta backup and all delta backups with the same base backup`  //nolint:lll

	DeleteEverythingUsageExample = "everything [FORCE]"
	DeleteRetainUsageExample     = "retain [FULL|FIND_FULL] backup_count"
	DeleteBeforeUsageExample     = "before [FIND_FULL] backup_name|timestamp"
	DeleteTargetUsageExample     = "target [FIND_FULL] backup_name | --target-user-data <data>"

	DeleteTargetUserDataFlag        = "target-user-data"
	DeleteTargetUserDataDescription = "delete storage backup which has the specified user data"
)

var StringModifiers = []string{"FULL", "FIND_FULL"}
var StringModifiersDeleteEverything = []string{"FORCE"}
var errNotFound = errors.New("not found")
var errIncorrectArguments = errors.New("incorrect arguments")

// BackupObject represents
// the backup sentinel object uploaded on storage
type BackupObject interface {
	storage.Object
	GetBackupTime() time.Time
	GetBackupName() string

	// TODO: move increment info into separate struct (in backup.go)
	IsFullBackup() bool
	GetBaseBackupName() string
	GetIncrementFromName() string
}

type DeleteHandlerOption func(h *DeleteHandler)

func IsPermanentFunc(isPermanent func(storage.Object) bool) DeleteHandlerOption {
	return func(h *DeleteHandler) {
		h.isPermanent = isPermanent
	}
}

func IsIgnoredFunc(isIgnored func(storage.Object) bool) DeleteHandlerOption {
	return func(h *DeleteHandler) {
		h.isIgnored = isIgnored
	}
}

func NewDeleteHandler(
	folder storage.Folder,
	backups []BackupObject,
	less func(object1, object2 storage.Object) bool,
	options ...DeleteHandlerOption,
) *DeleteHandler {
	deleteHandler := &DeleteHandler{
		Folder:  folder,
		backups: backups,
		less:    less,
		greater: func(object1, object2 storage.Object) bool {
			return less(object2, object1)
		},
		// by default, all storage objects are impermanent
		isPermanent: func(storage.Object) bool { return false },
		// by default, all storage objects are not ignored
		isIgnored: func(storage.Object) bool { return false },
	}

	for _, option := range options {
		option(deleteHandler)
	}

	return deleteHandler
}

type DeleteHandler struct {
	Folder  storage.Folder
	backups []BackupObject

	less    func(object1, object2 storage.Object) bool
	greater func(object1, object2 storage.Object) bool

	isPermanent func(object storage.Object) bool
	isIgnored   func(object storage.Object) bool
}

func (h *DeleteHandler) HandleDeleteBefore(args []string, confirmed bool) {
	modifier, beforeStr := ExtractDeleteModifierFromArgs(args)

	target, err := h.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(args []string, confirmed bool) {
	modifier, retentionStr := ExtractDeleteModifierFromArgs(args)
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
	modifier, retentionSir, afterStr := ExtractDeleteRetainAfterModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetainAfter(retentionCount, afterStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteTarget(targetSelector BackupSelector, confirmed, findFull bool) {
	targetName, err := targetSelector.Select(h.Folder)
	tracelog.ErrorLogger.FatalOnError(err)

	var target BackupObject
	for idx := range h.backups {
		if h.backups[idx].GetBackupName() == targetName {
			target = h.backups[idx]
			break
		}
	}

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	var backupsToDelete []BackupObject
	if findFull {
		// delete all backups with the same base backup as the target
		backupsToDelete = h.findRelatedBackups(target)
	} else {
		// delete all dependant backups
		backupsToDelete = h.findDependantBackups(target)
	}

	err = h.DeleteTargets(backupsToDelete, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteEverything(args []string, permanentBackups map[string]bool, confirmed bool) {
	forceModifier := false
	modifier := ExtractDeleteEverythingModifierFromArgs(args)
	if modifier == ForceDeleteModifier {
		forceModifier = true
	}

	if len(permanentBackups) > 0 {
		if !forceModifier {
			tracelog.ErrorLogger.Fatalf("Found permanent backups=%v\n", permanentBackups)
		}
		tracelog.InfoLogger.Printf("Found permanent backups=%v\n", permanentBackups)
	}
	h.DeleteEverything(confirmed)
}

func (h *DeleteHandler) FindTargetBefore(beforeStr string, modifier int) (BackupObject, error) {
	timeLine, err := time.Parse(time.RFC3339, beforeStr)
	if err == nil {
		return h.FindTargetBeforeTime(timeLine, modifier)
	}

	return h.FindTargetBeforeName(beforeStr, modifier)
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
	if err != nil && err != errNotFound {
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
	target, err := findTarget(h.backups, h.greater, choiceFunc)
	// it is OK to have no backups found outside of the specified retain window, skip this error
	if err != nil && err != errNotFound {
		return nil, err
	}
	return target, nil
}

func (h *DeleteHandler) FindTargetByName(bname string) (BackupObject, error) {
	return findTarget(h.backups, h.greater, func(object BackupObject) bool {
		return strings.HasPrefix(object.GetName(), bname)
	})
}

func (h *DeleteHandler) FindTargetRetainAfter(retentionCount int, afterStr string, modifier int) (BackupObject, error) {
	timeLine, err := time.Parse(time.RFC3339, afterStr)
	if err == nil {
		return h.FindTargetRetainAfterTime(retentionCount, timeLine, modifier)
	}

	return h.FindTargetRetainAfterName(retentionCount, afterStr, modifier)
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
		}
		return meetName && object.IsFullBackup()
	}

	target1, err := findTarget(h.backups, h.greater, choiceFuncRetain)
	if err != nil && err != errNotFound {
		return nil, err
	}
	target2, err := findTarget(h.backups, h.less, choiceFuncAfterName)
	if err != nil && err != errNotFound {
		return nil, err
	}

	if h.greater(target2, target1) {
		return target1, nil
	}
	return target2, nil
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
		}
		return timeCheck && object.IsFullBackup()
	}

	target1, err := findTarget(h.backups, h.greater, choiceFuncRetain)
	if err != nil && err != errNotFound {
		return nil, err
	}
	target2, err := findTarget(h.backups, h.less, choiceFuncAfter)
	if err != nil && err != errNotFound {
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
	folderFilter := func(path string) bool { return true }
	err := storage.DeleteObjectsWhere(h.Folder, confirmed, filter, folderFilter)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) DeleteBeforeTarget(target BackupObject, confirmed bool) error {
	return h.DeleteBeforeTargetWhere(target, confirmed, func(storage.Object) bool { return true })
}

func (h *DeleteHandler) DeleteBeforeTargetWhere(target BackupObject, confirmed bool, selector func(object storage.Object) bool) error {
	if !target.IsFullBackup() {
		errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
		return utility.NewForbiddenActionError(fmt.Sprintf(errorMessage, target.GetName()))
	}
	tracelog.InfoLogger.Println("Start delete")

	folderFilter := func(path string) bool { return true }
	return storage.DeleteObjectsWhere(h.Folder, confirmed, func(object storage.Object) bool {
		return selector(object) && h.less(object, target) && !h.isPermanent(object) && !h.isIgnored(object)
	}, folderFilter)
}

func (h *DeleteHandler) DeleteTargets(targets []BackupObject, confirmed bool) error {
	backupNamesToDelete := make(map[string]bool)
	for _, target := range targets {
		if h.isPermanent(target) {
			tracelog.ErrorLogger.Fatalf("Unable to delete permanent backup %s\n", target.GetName())
		}
		backupNamesToDelete[target.GetBackupName()] = true
	}

	folderFilter := func(path string) bool { return true }
	return storage.DeleteObjectsWhere(h.Folder.GetSubFolder(utility.BaseBackupPath),
		confirmed, func(object storage.Object) bool {
			return backupNamesToDelete[utility.StripLeftmostBackupName(object.GetName())] && !h.isPermanent(object) && !h.isIgnored(object)
		}, folderFilter)
}

// Find all backups related to the target.
// All delta backups with the same base backup are considered as related.
func (h *DeleteHandler) findRelatedBackups(target BackupObject) []BackupObject {
	relatedBackups := make([]BackupObject, 0)

	var related func(target BackupObject, other BackupObject) bool
	if target.IsFullBackup() {
		related = func(target BackupObject, other BackupObject) bool {
			// remove base backup
			isBaseBackup := target.GetBackupName() == other.GetBackupName()
			// remove all increments from the target backup too
			isIncrement := target.GetBackupName() == other.GetBaseBackupName()
			return isBaseBackup || isIncrement
		}
	} else {
		related = func(target BackupObject, other BackupObject) bool {
			// remove base backup
			isBaseBackup := target.GetBaseBackupName() == other.GetBackupName()
			// remove all other increments from the target base backup
			hasCommonBaseBackup := target.GetBaseBackupName() == other.GetBaseBackupName()
			return isBaseBackup || hasCommonBaseBackup
		}
	}

	for _, backup := range h.backups {
		if related(target, backup) {
			relatedBackups = append(relatedBackups, backup)
		}
	}
	return relatedBackups
}

// Find all backups dependant on the target.
// All delta backups which have the target as the ancestor in increment chain
// are considered as dependant.
func (h *DeleteHandler) findDependantBackups(target BackupObject) []BackupObject {
	relatedBackups := make([]BackupObject, 0)

	incrementsByBackup := make(map[string][]BackupObject)
	for _, backup := range h.backups {
		if !backup.IsFullBackup() {
			incrementFrom := backup.GetIncrementFromName()
			incrementsByBackup[incrementFrom] = append(incrementsByBackup[incrementFrom], backup)
		}
	}

	queue := []BackupObject{target}
	var curr BackupObject
	for len(queue) > 0 {
		curr, queue = queue[0], queue[1:]
		relatedBackups = append(relatedBackups, curr)
		queue = append(queue, incrementsByBackup[curr.GetBackupName()]...)
	}
	return relatedBackups
}

func findTarget(objects []BackupObject,
	compare func(object1, object2 storage.Object) bool,
	isTarget func(object BackupObject) bool) (BackupObject, error) {
	sort.Slice(objects, func(i, j int) bool {
		return compare(objects[i], objects[j])
	})
	for _, object := range objects {
		tracelog.DebugLogger.Printf("processing %s\n", object.GetName())
		if isTarget(object) {
			return object, nil
		}
	}
	return nil, errNotFound
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

			return count == retentionCount
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

// ExtractDeleteRetainAfterModifierFromArgs extracts the args for the "delete retain --after" command
func ExtractDeleteRetainAfterModifierFromArgs(args []string) (int, string, string) {
	if len(args) == 2 {
		return NoDeleteModifier, args[0], args[1]
	} else if args[0] == StringModifiers[0] {
		return FullDeleteModifier, args[1], args[2]
	}
	return FindFullDeleteModifier, args[1], args[2]
}

// ExtractDeleteEverythingModifierFromArgs extracts the args for the "delete everything" command
func ExtractDeleteEverythingModifierFromArgs(args []string) int {
	if len(args) == 0 {
		return NoDeleteModifier
	}
	return ForceDeleteModifier
}

// ExtractDeleteTargetModifierFromArgs extracts the args for the "delete target" command
func ExtractDeleteTargetModifierFromArgs(args []string) int {
	if len(args) >= 1 && args[0] == StringModifiers[1] {
		return FindFullDeleteModifier
	}

	return NoDeleteModifier
}

// ExtractDeleteModifierFromArgs extracts the delete modifier the "delete retain"/"delete before" commands
func ExtractDeleteModifierFromArgs(args []string) (int, string) {
	if len(args) == 1 {
		return NoDeleteModifier, args[0]
	} else if args[0] == StringModifiers[0] {
		return FullDeleteModifier, args[1]
	}
	return FindFullDeleteModifier, args[1]
}

func DeleteBeforeArgsValidator(cmd *cobra.Command, args []string) error {
	err := DeleteArgsValidator(args, StringModifiers, 1, 2)
	if err != nil {
		return err
	}
	modifier, beforeStr := ExtractDeleteModifierFromArgs(args)
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

func DeleteTargetArgsValidator(cmd *cobra.Command, args []string) error {
	err := cobra.RangeArgs(0, 2)(cmd, args)
	if err != nil {
		return err
	}

	switch {
	case len(args) == 0 && !cmd.Flags().Changed(DeleteTargetUserDataFlag):
		// allow 0 arguments only when target user data flag is set
		return errIncorrectArguments

	case len(args) == 2 && args[0] != StringModifiers[1]:
		return errIncorrectArguments

	default:
		return nil
	}
}

func DeleteEverythingArgsValidator(cmd *cobra.Command, args []string) error {
	return DeleteArgsValidator(args, StringModifiersDeleteEverything, 0, 1)
}

func DeleteRetainArgsValidator(cmd *cobra.Command, args []string) error {
	_, retentionStr := ExtractDeleteModifierFromArgs(args)
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
	err := DeleteArgsValidator(args, StringModifiers, 2, 3)
	if err != nil {
		return err
	}
	_, retentionStr, afterStr := ExtractDeleteRetainAfterModifierFromArgs(args)
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

func DeleteArgsValidator(args, stringModifiers []string, minArgs int, maxArgs int) error {
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
