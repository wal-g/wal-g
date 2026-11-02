package internal

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/multistorage"
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
  target FIND_FULL base_0000000100000000000000C9_D_0000000100000000000000C4	delete delta backup and all delta backups with the same base backup` //nolint:lll

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
	multistorage.StorageTeller
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

// SkipFolderFunc excludes subtrees from ordinary cleanup before listing them.
// Paths are relative to the listing root. DeleteEverything ignores this option.
func SkipFolderFunc(skipFolder func(string) bool) DeleteHandlerOption {
	return func(h *DeleteHandler) {
		h.skipFolder = skipFolder
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
	skipFolder  func(name string) bool
}

func (h *DeleteHandler) filterFolders(selector func(string) bool) func(string) bool {
	if h.skipFolder == nil {
		return selector
	}
	return func(name string) bool {
		return !h.skipFolder(name) && selector(name)
	}
}

func (h *DeleteHandler) HandleDeleteBefore(ctx context.Context, args []string, confirmed bool) {
	modifier, beforeStr := ExtractDeleteModifierFromArgs(args)

	target, err := h.FindTargetBefore(beforeStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(ctx, target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetain(ctx context.Context, args []string, confirmed bool) {
	modifier, retentionStr := ExtractDeleteModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionStr)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetain(retentionCount, modifier)
	tracelog.ErrorLogger.FatalOnError(err)
	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}
	err = h.DeleteBeforeTarget(ctx, target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteRetainAfter(ctx context.Context, args []string, confirmed bool) {
	modifier, retentionSir, afterStr := ExtractDeleteRetainAfterModifierFromArgs(args)
	retentionCount, err := strconv.Atoi(retentionSir)
	tracelog.ErrorLogger.FatalOnError(err)

	target, err := h.FindTargetRetainAfter(retentionCount, afterStr, modifier)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		tracelog.InfoLogger.Printf("No backup found for deletion")
		os.Exit(0)
	}

	err = h.DeleteBeforeTarget(ctx, target, confirmed)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) HandleDeleteTarget(ctx context.Context, targetSelector BackupSelector, confirmed, findFull bool) BackupObject {
	target, err := h.FindTargetBySelector(ctx, targetSelector)
	tracelog.ErrorLogger.FatalOnError(err)

	if target == nil {
		// since we want to delete the target backup, we should fail if
		// we didn't find the requested backup for deletion
		tracelog.ErrorLogger.Fatal("Requested backup was not found")
	}

	folderFilter := func(name string) bool { return true }
	err = h.DeleteTargetCopies(ctx, target, confirmed, findFull, folderFilter)
	tracelog.ErrorLogger.FatalOnError(err)

	return target
}

func (h *DeleteHandler) HandleDeleteEverything(ctx context.Context, args []string, permanentBackups []string, confirmed bool) {
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
	h.DeleteEverything(ctx, confirmed)
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
	// it is OK to have no backups found outside the specified retain window, skip this error
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

// TODO: unit tests
func (h *DeleteHandler) FindTargetBySelector(ctx context.Context, targetSelector BackupSelector) (BackupObject, error) {
	targetBackup, err := targetSelector.Select(ctx, h.Folder)
	if err != nil {
		if _, ok := err.(NoBackupsFoundError); ok {
			// it is OK to have no backups found for the provided selector,
			// just return no target found
			return nil, nil
		}
		return nil, err
	}

	var target BackupObject
	for idx := range h.backups {
		if h.backups[idx].GetBackupName() == targetBackup.Name {
			target = h.backups[idx]
			break
		}
	}

	return target, nil
}

// TODO: unit tests
func (h *DeleteHandler) FindTargetRetainAfter(retentionCount int, afterStr string, modifier int) (BackupObject, error) {
	timeLine, err := time.Parse(time.RFC3339, afterStr)
	if err == nil {
		return h.FindTargetRetainAfterTime(retentionCount, timeLine, modifier)
	}

	return h.FindTargetRetainAfterName(retentionCount, afterStr, modifier)
}

// TODO: unit tests
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

// TODO: unit tests
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

func (h *DeleteHandler) DeleteEverything(ctx context.Context, confirmed bool) {
	filter := func(object storage.Object) bool { return true }
	folderFilter := func(path string) bool { return true }
	err := DeleteObjectsWhere(ctx, h.Folder, confirmed, filter, folderFilter)
	tracelog.ErrorLogger.FatalOnError(err)
}

func (h *DeleteHandler) DeleteBeforeTarget(ctx context.Context, target BackupObject, confirmed bool) error {
	objFilter := func(storage.Object) bool { return true }
	folderFilter := func(string) bool { return true }

	return h.DeleteBeforeTargetWhere(ctx, target, confirmed, objFilter, folderFilter)
}

func (h *DeleteHandler) DeleteBeforeTargetWhere(
	ctx context.Context,
	target BackupObject,
	confirmed bool,
	objSelector func(object storage.Object) bool,
	folderFilter func(name string) bool,
) error {
	if !target.IsFullBackup() {
		errorMessage := "%v is incremental and it's predecessors cannot be deleted. Consider FIND_FULL option."
		return utility.NewForbiddenActionError(fmt.Sprintf(errorMessage, target.GetName()))
	}
	tracelog.InfoLogger.Println("Start delete")

	return DeleteObjectsWhere(ctx, h.Folder, confirmed, func(object storage.Object) bool {
		return objSelector(object) && h.less(object, target) && !h.isPermanent(object)
	}, h.filterFolders(folderFilter))
}

func (h *DeleteHandler) DeleteWhere(
	ctx context.Context,
	confirmed bool,
	objSelector func(object storage.Object) bool,
	folderFilter func(name string) bool,
) error {
	tracelog.InfoLogger.Println("Start delete")

	return DeleteObjectsWhere(ctx, h.Folder, confirmed, func(object storage.Object) bool {
		return objSelector(object) && !h.isPermanent(object)
	}, h.filterFolders(folderFilter))
}

func (h *DeleteHandler) DeleteTarget(ctx context.Context, target BackupObject, confirmed, findFull bool,
	folderFilter func(name string) bool) error {
	return h.deleteTargets(ctx, []BackupObject{target}, confirmed, findFull, folderFilter)
}

func (h *DeleteHandler) TargetCopies(target BackupObject) []BackupObject {
	targets := []BackupObject{target}
	for _, backup := range h.backups {
		if backup.GetBackupName() == target.GetBackupName() && backup.GetStorage() != target.GetStorage() {
			targets = append(targets, backup)
		}
	}
	return targets
}

func (h *DeleteHandler) DeleteTargetCopies(ctx context.Context, target BackupObject, confirmed, findFull bool,
	folderFilter func(string) bool) error {
	return h.deleteTargets(ctx, h.TargetCopies(target), confirmed, findFull, folderFilter)
}

type targetDeletion struct {
	folder      storage.Folder
	storageName string
	backupNames map[string]bool
}

type TargetDeletionPlan struct {
	Backups      []BackupObject
	handler      *DeleteHandler
	destinations []targetDeletion
	folderFilter func(string) bool
}

func (h *DeleteHandler) PrepareDeleteTargets(ctx context.Context, targets []BackupObject, findFull bool,
	folderFilter func(string) bool) (*TargetDeletionPlan, error) {
	if err := ValidateBackupDependencies(h.backups); err != nil {
		return nil, err
	}
	plan := &TargetDeletionPlan{handler: h, folderFilter: folderFilter}
	indexes := map[string]int{}
	for _, target := range targets {
		destination := target.GetStorage()
		index, exists := indexes[destination]
		if !exists {
			folder, err := multistorage.UseSpecificStorage(ctx, destination, h.Folder)
			if err != nil {
				return nil, err
			}
			index = len(plan.destinations)
			indexes[destination] = index
			plan.destinations = append(plan.destinations, targetDeletion{
				folder.GetSubFolder(utility.BaseBackupPath), destination, map[string]bool{},
			})
		}
		var backups []BackupObject
		if findFull {
			backups = h.findRelatedBackups(target)
		} else {
			backups = h.findDependantBackups(target)
		}
		for _, backup := range backups {
			if h.isPermanent(deletionBackupObject{backup}) {
				return nil, fmt.Errorf("unable to delete permanent backup %s", backup.GetName())
			}
			names := plan.destinations[index].backupNames
			if !names[backup.GetBackupName()] {
				names[backup.GetBackupName()] = true
				plan.Backups = append(plan.Backups, backup)
			}
		}
	}
	return plan, nil
}

func (p *TargetDeletionPlan) Execute(ctx context.Context, confirmed bool) error {
	for _, destination := range p.destinations {
		err := DeleteObjectsWhere(ctx, destination.folder, confirmed, func(object storage.Object) bool {
			return multistorage.GetStorage(object) == destination.storageName &&
				destination.backupNames[utility.StripLeftmostBackupName(object.GetName())] && !p.handler.isPermanent(object)
		}, p.handler.filterFolders(p.folderFilter))
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *DeleteHandler) deleteTargets(ctx context.Context, targets []BackupObject, confirmed, findFull bool,
	folderFilter func(string) bool) error {
	plan, err := h.PrepareDeleteTargets(ctx, targets, findFull, folderFilter)
	if err != nil {
		return err
	}
	return plan.Execute(ctx, confirmed)
}

func (h *DeleteHandler) TargetBackupNames(target BackupObject, findFull bool) []string {
	backups := h.findDependantBackups(target)
	if findFull {
		backups = h.findRelatedBackups(target)
	}
	result := make([]string, 0, len(backups))
	for _, b := range backups {
		result = append(result, b.GetBackupName())
	}
	return result
}

func (h *DeleteHandler) BackupNamesBefore(target BackupObject, storageName string) []string {
	var result []string
	for _, b := range h.backups {
		if b.GetStorage() == storageName && h.less(b, target) && !h.isPermanent(deletionBackupObject{b}) {
			result = append(result, b.GetBackupName())
		}
	}
	return result
}

// TODO: unit tests
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
		if backup.GetStorage() == target.GetStorage() && related(target, backup) {
			relatedBackups = append(relatedBackups, backup)
		}
	}
	return relatedBackups
}

// TODO: unit tests
// Find all backups dependant on the target.
// All delta backups which have the target as the ancestor in increment chain
// are considered as dependant.
func (h *DeleteHandler) findDependantBackups(target BackupObject) []BackupObject {
	dependantBackups := make([]BackupObject, 0)

	incrementsByBackup := make(map[string][]BackupObject)
	for _, backup := range h.backups {
		if backup.GetStorage() != target.GetStorage() {
			continue
		}
		if !backup.IsFullBackup() {
			incrementFrom := backup.GetIncrementFromName()
			incrementsByBackup[incrementFrom] = append(incrementsByBackup[incrementFrom], backup)
		}
	}

	queue := []BackupObject{target}
	visited := map[string]bool{}
	var curr BackupObject
	for len(queue) > 0 {
		curr, queue = queue[0], queue[1:]
		if visited[curr.GetBackupName()] {
			continue
		}
		visited[curr.GetBackupName()] = true
		dependantBackups = append(dependantBackups, curr)
		queue = append(queue, incrementsByBackup[curr.GetBackupName()]...)
	}
	return dependantBackups
}

func DeleteObjectsWhere(
	ctx context.Context,
	folder storage.Folder,
	confirm bool,
	objFilter func(object1 storage.Object) bool,
	folderFilter func(name string) bool,
) error {
	// if folder has uncurrent versions we need to clean them as well
	folder = storage.WithAllVersions(folder)
	relativePathObjects, err := multistorage.ListFolderRecursivelyWithFilter(ctx, folder, folderFilter)
	if err != nil {
		return err
	}
	markedForDeletion := make([]storage.Object, 0, len(relativePathObjects))
	tracelog.InfoLogger.Println("Evaluating objects for deletion...")
	for _, object := range relativePathObjects {
		if objFilter(object) {
			tracelog.InfoLogger.Printf("Object marked for deletion: %s storage=%s\n", object.GetName(), multistorage.GetStorage(object))
			markedForDeletion = append(markedForDeletion, object)
		} else {
			tracelog.DebugLogger.Printf("Object skipped: %s storage=%s\n", object.GetName(), multistorage.GetStorage(object))
		}
	}
	deletionCount := len(markedForDeletion)
	if deletionCount == 0 {
		tracelog.InfoLogger.Println("No objects matched the deletion criteria.")
		return nil
	}
	if confirm {
		err := deleteObjectsPhased(ctx, folder, markedForDeletion)
		if err == nil {
			tracelog.InfoLogger.Printf("Objects deleted successfully: count=%d\n", deletionCount)
		}
		return err
	}
	tracelog.InfoLogger.Printf("Dry run: objects would be deleted count=%d, Run with --confirm to execute\n", deletionCount)
	return nil
}

func findTarget(objects []BackupObject,
	compare func(object1, object2 storage.Object) bool,
	isTarget func(object BackupObject) bool) (BackupObject, error) {
	slices.SortFunc(objects, func(a, b BackupObject) int {
		if compare(a, b) {
			return -1
		}
		if compare(b, a) {
			return 1
		}
		return 0
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
	uniqueNames := map[string]bool{}
	switch modifier {
	case NoDeleteModifier:
		return func(object BackupObject) bool {
			uniqueNames[object.GetBackupName()] = true

			return len(uniqueNames) == retentionCount
		}
	case FullDeleteModifier:
		return func(object BackupObject) bool {
			if object.IsFullBackup() {
				uniqueNames[object.GetBackupName()] = true
			}
			if len(uniqueNames) == retentionCount {
				return true
			}
			return false
		}
	case FindFullDeleteModifier:
		return func(object BackupObject) bool {
			uniqueNames[object.GetBackupName()] = true
			if len(uniqueNames) >= retentionCount && object.IsFullBackup() {
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
		if !slices.Contains(stringModifiers, args[0]) {
			return fmt.Errorf("expected to get one of modifiers: %v as first argument", stringModifiers)
		}
	}
	return nil
}
